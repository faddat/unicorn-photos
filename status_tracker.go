package main

import (
	"encoding/json"
	"fmt" // Added for fmt.Errorf
	"os"
	"path/filepath"
	"strings" // Added for LogStatusSummary formatting
	"sync"
	"time"
)

// ChainStatus represents the snapshot status of a single chain
type ChainStatus struct {
	ChainID               string            `json:"chain_id"`
	Name                  string            `json:"name"`
	LastAttemptedHeight   int64             `json:"last_attempted_height"`
	LastSuccessfulHeight  int64             `json:"last_successful_height"`
	LastSnapshotTimestamp time.Time         `json:"last_snapshot_timestamp"`
	SnapshotCount         int               `json:"snapshot_count"`
	Status                string            `json:"status"` // "active", "pending", "completed", "error", "unknown"
	LastError             string            `json:"last_error,omitempty"`
	LastProgress          *SnapshotProgress `json:"last_progress,omitempty"` // Current snapshot progress
}

// SnapshotProgress tracks the progress of a snapshot operation
type SnapshotProgress struct {
	ChainID           string    `json:"chain_id"`
	Height            int64     `json:"height"`
	StartTime         time.Time `json:"start_time"`
	CurrentModule     string    `json:"current_module"`
	CompletedModules  []string  `json:"completed_modules"`
	TotalModules      int       `json:"total_modules"`
	PercentComplete   int       `json:"percent_complete"`
	ModuleInProgress  bool      `json:"module_in_progress"`   // Not currently used, consider removal
	CurrentItemCount  int       `json:"current_item_count"`   // Not currently used, consider removal
	TotalItemsInBatch int       `json:"total_items_in_batch"` // Not currently used, consider removal
}

// StatusTracker tracks the status of all chains
type StatusTracker struct {
	mu               sync.RWMutex
	StatusFilePath   string                 `json:"-"` // Should be set by daemon after config load
	StartTime        time.Time              `json:"start_time"`
	LastUpdateTime   time.Time              `json:"last_update_time"`
	TotalChains      int                    `json:"total_chains"`
	ActiveChains     int                    `json:"active_chains"`      // Chains currently being snapshotted
	CompletedChains  int                    `json:"completed_chains"`   // Chains that finished a snapshot and are idle
	PendingChains    int                    `json:"pending_chains"`     // Chains registered but not yet processed or awaiting retry
	ChainsWithErrors int                    `json:"chains_with_errors"` // Chains that encountered an error in last attempt
	TotalSnapshots   int                    `json:"total_snapshots"`    // Cumulative count of successful snapshots across all chains
	ChainStatuses    map[string]ChainStatus `json:"chain_statuses"`

	stopCh chan struct{} `json:"-"` // For stopping periodic updates
}

var globalStatusTracker *StatusTracker
var statusTrackerOnce sync.Once

// GetStatusTracker returns the global status tracker instance.
func GetStatusTracker() *StatusTracker {
	statusTrackerOnce.Do(func() {
		globalStatusTracker = NewStatusTracker()
	})
	return globalStatusTracker
}

// NewStatusTracker creates a new status tracker.
// StatusFilePath will be set by the daemon after config is loaded.
func NewStatusTracker() *StatusTracker {
	// Temporary default path, will be overwritten by daemon using config.
	defaultPath := filepath.Join("snapshots", "status.json")

	return &StatusTracker{
		StatusFilePath: defaultPath,
		StartTime:      time.Now(),
		LastUpdateTime: time.Now(),
		ChainStatuses:  make(map[string]ChainStatus),
		stopCh:         make(chan struct{}),
	}
}

// Start begins periodic status updates.
// Assumes StatusFilePath has been correctly set by the caller (e.g., daemon).
func (st *StatusTracker) Start() {
	// Ensure directory for status file exists before starting periodic updates.
	statusDir := filepath.Dir(st.StatusFilePath)
	if _, statErr := os.Stat(statusDir); os.IsNotExist(statErr) {
		if mkErr := os.MkdirAll(statusDir, 0755); mkErr != nil {
			logger.Printf("CRITICAL: Failed to create directory for status file %s: %v. Status updates will likely fail.", st.StatusFilePath, mkErr)
			// Depending on desired robustness, could panic or try to continue without saving.
		}
	}
	go st.periodicStatusUpdates()
}

// Stop halts the periodic status updates.
func (st *StatusTracker) Stop() {
	select {
	case <-st.stopCh:
		// Already closed
		return
	default:
		close(st.stopCh)
	}
}

// _saveStatusFile_nolock performs the actual saving, assuming the caller holds the appropriate lock.
func (st *StatusTracker) _saveStatusFile_nolock() error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		logger.Printf("Error marshalling status data: %v", err)
		return err
	}

	statusDir := filepath.Dir(st.StatusFilePath)
	if _, statErr := os.Stat(statusDir); os.IsNotExist(statErr) {
		if mkErr := os.MkdirAll(statusDir, 0755); mkErr != nil {
			logger.Printf("Error creating directory for status file %s: %v", st.StatusFilePath, mkErr)
			return fmt.Errorf("failed to create directory for status file %s: %w", st.StatusFilePath, mkErr)
		}
	}

	err = os.WriteFile(st.StatusFilePath, data, 0644)
	if err != nil {
		logger.Printf("Error writing status file %s: %v", st.StatusFilePath, err)
	}
	return err
}

// SaveStatusFile is the public method for saving, typically used by periodic updaters or external callers.
// It acquires an RLock because it's only reading `st` for marshalling.
func (st *StatusTracker) SaveStatusFile() error {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st._saveStatusFile_nolock()
}

func (st *StatusTracker) periodicStatusUpdates() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := st.SaveStatusFile(); err != nil { // Uses public SaveStatusFile with RLock
				logger.Printf("Error in periodic status save: %v", err)
			}
			st.LogStatusSummary()
		case <-st.stopCh:
			logger.Printf("Stopping periodic status updates. Performing final save.")
			if err := st.SaveStatusFile(); err != nil {
				logger.Printf("Error in final status save: %v", err)
			}
			return
		}
	}
}

// RegisterChain adds a new chain to the tracker or ensures it's known.
func (st *StatusTracker) RegisterChain(chainID, name string) {
	st.mu.Lock() // WLock
	defer st.mu.Unlock()

	if _, exists := st.ChainStatuses[chainID]; !exists {
		st.ChainStatuses[chainID] = ChainStatus{
			ChainID: chainID,
			Name:    name,
			Status:  "pending", // Initial status: registered, awaiting first snapshot attempt
		}
		st.TotalChains++
		st.PendingChains++ // It's now in a 'pending' state.
		st.LastUpdateTime = time.Now()
		if err := st._saveStatusFile_nolock(); err != nil { // Call internal save method
			logger.Printf("Error saving status after registering chain %s: %v", chainID, err)
		}
	}
}

// UpdateChainStatus updates the status of a specific chain.
func (st *StatusTracker) UpdateChainStatus(chainID string, newChainStatus ChainStatus) {
	st.mu.Lock() // WLock
	defer st.mu.Unlock()

	oldStatus, exists := st.ChainStatuses[chainID]

	if !exists {
		// This case implies a chain status is being updated before it was explicitly registered.
		// This can happen if the first event is a snapshot attempt.
		st.TotalChains++
		// Initialize oldStatus to a zero-value or a default "unknown" if necessary
		// for count adjustments below. For now, `exists` check handles it.
		logger.Printf("Info: Updating status for chain %s which was not previously registered. Registering now.", chainID)
		// Ensure Name is populated if newChainStatus doesn't have it but it's known
		if newChainStatus.Name == "" && oldStatus.Name != "" { // oldStatus would be zero here if !exists
			// This Name logic might be better handled by ensuring newChainStatus always has Name.
		}
	}

	// Update TotalSnapshots: if newChainStatus.SnapshotCount increased, add the difference.
	// SnapshotCount in ChainStatus should reflect the total successful snapshots for that chain.
	if exists {
		if newChainStatus.SnapshotCount > oldStatus.SnapshotCount {
			st.TotalSnapshots += (newChainStatus.SnapshotCount - oldStatus.SnapshotCount)
		}
	} else if newChainStatus.SnapshotCount > 0 {
		// If chain didn't exist and now has snapshots.
		st.TotalSnapshots += newChainStatus.SnapshotCount
	}

	// Adjust aggregate counts based on status transition
	if exists { // Decrement count for the old status
		switch oldStatus.Status {
		case "active":
			st.ActiveChains--
		case "pending":
			st.PendingChains--
		case "completed":
			st.CompletedChains--
		case "error":
			st.ChainsWithErrors--
		}
	}

	// Increment count for the new status
	switch newChainStatus.Status {
	case "active":
		st.ActiveChains++
	case "pending":
		st.PendingChains++
	case "completed":
		st.CompletedChains++
	case "error":
		st.ChainsWithErrors++
		// case "unknown" or other states are not counted in aggregates for now
	}

	// Sanity check for counts
	if st.ActiveChains < 0 {
		st.ActiveChains = 0
	}
	if st.PendingChains < 0 {
		st.PendingChains = 0
	}
	if st.CompletedChains < 0 {
		st.CompletedChains = 0
	}
	if st.ChainsWithErrors < 0 {
		st.ChainsWithErrors = 0
	}

	st.ChainStatuses[chainID] = newChainStatus
	st.LastUpdateTime = time.Now()
	if err := st._saveStatusFile_nolock(); err != nil { // Call internal save method
		logger.Printf("Error saving status after updating chain %s: %v", chainID, err)
	}
}

// LoadStatusFile loads the status from disk if available.
// Assumes StatusFilePath has been correctly set by the caller.
func (st *StatusTracker) LoadStatusFile() error {
	data, err := os.ReadFile(st.StatusFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Printf("Status file %s not found. Starting fresh.", st.StatusFilePath)
			return nil // Not an error if file doesn't exist.
		}
		logger.Printf("Error reading status file %s: %v", st.StatusFilePath, err)
		return err
	}

	st.mu.Lock() // WLock for unmarshalling into st
	defer st.mu.Unlock()

	err = json.Unmarshal(data, st)
	if err != nil {
		logger.Printf("Error unmarshalling status file %s: %v. Starting fresh.", st.StatusFilePath, err)
		// Reset to a known good state if unmarshalling fails
		st.StartTime = time.Now()
		st.LastUpdateTime = time.Now()
		st.ChainStatuses = make(map[string]ChainStatus)
		st.TotalChains = 0
		st.ActiveChains = 0
		st.CompletedChains = 0
		st.PendingChains = 0
		st.ChainsWithErrors = 0
		st.TotalSnapshots = 0
		// stopCh is transient, will be re-created if Start is called.
		// Ensure it's at least non-nil.
		if st.stopCh == nil {
			st.stopCh = make(chan struct{})
		}
		return nil // Treat as "starting fresh" rather than fatal.
	}
	// Successfully loaded, ensure stopCh is new as it's not marshalled from JSON.
	// It should be re-created by NewStatusTracker or when Start is called.
	// For safety, ensure it's a new, open channel.
	st.stopCh = make(chan struct{})
	logger.Printf("Successfully loaded status from %s", st.StatusFilePath)
	return nil
}

// LogStatusSummary logs a summary of the current snapshot status.
func (st *StatusTracker) LogStatusSummary() {
	st.mu.RLock() // RLock for reading
	defer st.mu.RUnlock()

	logger.Printf("SNAPSHOT STATUS SUMMARY:")
	logger.Printf("  Total chains: %d (Active: %d, Completed: %d, Pending: %d, Errors: %d)",
		st.TotalChains, st.ActiveChains, st.CompletedChains, st.PendingChains, st.ChainsWithErrors)
	logger.Printf("  Total successful snapshots: %d", st.TotalSnapshots)
	logger.Printf("  Daemon running since: %s (Last status update: %s)",
		st.StartTime.Format(time.RFC3339), st.LastUpdateTime.Format(time.RFC3339))

	if st.ChainsWithErrors > 0 {
		var errorMessages []string
		for _, status := range st.ChainStatuses {
			if status.Status == "error" {
				errorMessages = append(errorMessages, fmt.Sprintf("    %s (%s): %s", status.Name, status.ChainID, status.LastError))
			}
		}
		if len(errorMessages) > 0 {
			logger.Printf("  Chains with errors:\n%s", strings.Join(errorMessages, "\n"))
		}
	}
}

// GetStatusSummary returns a simple status summary.
func (st *StatusTracker) GetStatusSummary() map[string]interface{} {
	st.mu.RLock()
	defer st.mu.RUnlock()

	return map[string]interface{}{
		"total_chains":       st.TotalChains,
		"active_chains":      st.ActiveChains,
		"completed_chains":   st.CompletedChains,
		"pending_chains":     st.PendingChains,
		"chains_with_errors": st.ChainsWithErrors,
		"total_snapshots":    st.TotalSnapshots,
		"running_since":      st.StartTime,
		"last_update":        st.LastUpdateTime,
	}
}

// GetChainStatus returns the status for a specific chain.
func (st *StatusTracker) GetChainStatus(chainID string) ChainStatus {
	st.mu.RLock()
	defer st.mu.RUnlock()

	if status, exists := st.ChainStatuses[chainID]; exists {
		return status
	}
	// Return a default status if not found (e.g., for a chain not yet registered)
	return ChainStatus{
		ChainID: chainID,
		Status:  "unknown",
	}
}
