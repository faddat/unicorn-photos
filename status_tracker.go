package main

import (
	"encoding/json"
	"os"
	"path/filepath"
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
	Status                string            `json:"status"` // "active", "pending", "error", etc.
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
	ModuleInProgress  bool      `json:"module_in_progress"`
	CurrentItemCount  int       `json:"current_item_count"`
	TotalItemsInBatch int       `json:"total_items_in_batch"`
}

// StatusTracker tracks the status of all chains
type StatusTracker struct {
	mu               sync.RWMutex
	StatusFilePath   string                 `json:"-"`
	StartTime        time.Time              `json:"start_time"`
	LastUpdateTime   time.Time              `json:"last_update_time"`
	TotalChains      int                    `json:"total_chains"`
	ActiveChains     int                    `json:"active_chains"`
	CompletedChains  int                    `json:"completed_chains"`
	PendingChains    int                    `json:"pending_chains"`
	ChainsWithErrors int                    `json:"chains_with_errors"`
	TotalSnapshots   int                    `json:"total_snapshots"`
	ChainStatuses    map[string]ChainStatus `json:"chain_statuses"`

	// Add stopCh to track when to stop status updates
	stopCh chan struct{} `json:"-"`
}

var globalStatusTracker *StatusTracker
var statusTrackerOnce sync.Once

// GetStatusTracker returns the global status tracker instance
func GetStatusTracker() *StatusTracker {
	statusTrackerOnce.Do(func() {
		globalStatusTracker = NewStatusTracker()
	})
	return globalStatusTracker
}

// NewStatusTracker creates a new status tracker
func NewStatusTracker() *StatusTracker {
	baseDir := "snapshots"
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		os.MkdirAll(baseDir, 0755)
	}

	statusFilePath := filepath.Join(baseDir, "status.json")

	return &StatusTracker{
		StatusFilePath: statusFilePath,
		StartTime:      time.Now(),
		LastUpdateTime: time.Now(),
		ChainStatuses:  make(map[string]ChainStatus),
		stopCh:         make(chan struct{}),
	}
}

// Start begins periodic status updates
func (st *StatusTracker) Start() {
	go st.periodicStatusUpdates()
}

// Stop halts the periodic status updates
func (st *StatusTracker) Stop() {
	close(st.stopCh)
}

// periodicStatusUpdates runs in the background, updating status file every minute
func (st *StatusTracker) periodicStatusUpdates() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			st.SaveStatusFile()
			st.LogStatusSummary()
		case <-st.stopCh:
			return
		}
	}
}

// RegisterChain adds a new chain to the tracker
func (st *StatusTracker) RegisterChain(chainID, name string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if _, exists := st.ChainStatuses[chainID]; !exists {
		st.ChainStatuses[chainID] = ChainStatus{
			ChainID: chainID,
			Name:    name,
			Status:  "pending",
		}
		st.TotalChains++
		st.PendingChains++
	}

	st.SaveStatusFile()
}

// UpdateChainStatus updates the status of a specific chain
func (st *StatusTracker) UpdateChainStatus(chainID string, status ChainStatus) {
	st.mu.Lock()
	defer st.mu.Unlock()

	oldStatus, exists := st.ChainStatuses[chainID]

	// If this is a new snapshot, increment the count
	if exists && status.LastSuccessfulHeight > oldStatus.LastSuccessfulHeight {
		status.SnapshotCount = oldStatus.SnapshotCount + 1
		st.TotalSnapshots++
	} else if !exists {
		if status.LastSuccessfulHeight > 0 {
			status.SnapshotCount = 1
			st.TotalSnapshots++
		}
	}

	// Update active/completed/error counts
	if exists {
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

	switch status.Status {
	case "active":
		st.ActiveChains++
	case "pending":
		st.PendingChains++
	case "completed":
		st.CompletedChains++
	case "error":
		st.ChainsWithErrors++
	}

	st.ChainStatuses[chainID] = status
	st.LastUpdateTime = time.Now()

	// Save to disk with each update
	st.SaveStatusFile()
}

// SaveStatusFile writes the current status to disk
func (st *StatusTracker) SaveStatusFile() error {
	st.mu.RLock()
	defer st.mu.RUnlock()

	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(st.StatusFilePath, data, 0644)
}

// LoadStatusFile loads the status from disk if available
func (st *StatusTracker) LoadStatusFile() error {
	data, err := os.ReadFile(st.StatusFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			// If file doesn't exist, that's okay, we'll create it
			return nil
		}
		return err
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	return json.Unmarshal(data, st)
}

// LogStatusSummary logs a summary of the current snapshot status
func (st *StatusTracker) LogStatusSummary() {
	st.mu.RLock()
	defer st.mu.RUnlock()

	logger.Printf("SNAPSHOT STATUS SUMMARY:")
	logger.Printf("Total chains: %d (Active: %d, Completed: %d, Pending: %d, Error: %d)",
		st.TotalChains, st.ActiveChains, st.CompletedChains, st.PendingChains, st.ChainsWithErrors)
	logger.Printf("Total snapshots taken: %d", st.TotalSnapshots)
	logger.Printf("Running since: %s", st.StartTime.Format(time.RFC3339))
	logger.Printf("Status file location: %s", st.StatusFilePath)

	// Log details of chains with errors
	if st.ChainsWithErrors > 0 {
		logger.Printf("Chains with errors:")
		for _, status := range st.ChainStatuses {
			if status.Status == "error" {
				logger.Printf("  %s (%s): %s", status.Name, status.ChainID, status.LastError)
			}
		}
	}
}

// GetStatusSummary returns a simple status summary
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

// GetChainStatus returns the status for a specific chain
func (st *StatusTracker) GetChainStatus(chainID string) ChainStatus {
	st.mu.RLock()
	defer st.mu.RUnlock()

	if status, exists := st.ChainStatuses[chainID]; exists {
		return status
	}

	// Return a default status if not found
	return ChainStatus{
		ChainID: chainID,
		Status:  "pending",
	}
}
