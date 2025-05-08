package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Command to display snapshot status information
func cmdStatus() error {
	cfg, err := LoadConfig()
	var baseDir string
	if err != nil {
		logger.Printf("Warning: Failed to load config for status command: %v. Using default snapshot path 'snapshots'.", err)
		baseDir = "snapshots" // Fallback to default
	} else {
		baseDir = cfg.SnapshotBaseDir
	}

	// Ensure the base directory for snapshots exists, as status.json is inside it.
	// This is important if `run` has never been executed.
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		// Try to create it. If this fails, status file operations will also likely fail.
		if err := os.MkdirAll(baseDir, 0755); err != nil {
			// Log the error but attempt to proceed; LoadStatusFile/SaveStatusFile might handle it
			logger.Printf("Warning: Failed to create snapshots directory %s: %v", baseDir, err)
		}
		// If baseDir didn't exist, it's unlikely status.json exists.
		// Display initial message.
		fmt.Println("=== Unicorn Photos - Status ===")
		fmt.Println("No snapshots have been taken yet (snapshot directory not found).")
		fmt.Println("\nTo start the snapshot daemon, run:")
		fmt.Println("  unicorn-photos run")
		fmt.Println("\nThis will initialize the status tracking system and begin snapshotting chains.")
		// Optionally, create a default status file here if desired, but it might be better
		// to let `run` command initialize it first.
		return nil
	}

	statusTracker := GetStatusTracker()
	// Set the correct StatusFilePath based on config (or default if config failed)
	statusTracker.StatusFilePath = filepath.Join(baseDir, "status.json")
	statusFilePath := statusTracker.StatusFilePath

	// Check if status file exists
	if _, err := os.Stat(statusFilePath); os.IsNotExist(err) {
		fmt.Println("=== Unicorn Photos - Status ===")
		fmt.Println("Snapshot daemon has not been started yet, or status file not found.")
		fmt.Println("\nTo start the snapshot daemon, run:")
		fmt.Println("  unicorn-photos run")
		fmt.Println("\nThis will initialize the status tracking system and begin snapshotting chains.")

		// Initialize a default status file for future `status` calls if desired
		// This involves marshalling the current (empty/new) statusTracker
		// Ensure directory for status file exists.
		statusDir := filepath.Dir(statusFilePath)
		if err := os.MkdirAll(statusDir, 0755); err != nil {
			return fmt.Errorf("failed to create directory for initial status file %s: %w", statusDir, err)
		}
		if err := statusTracker.SaveStatusFile(); err != nil { // SaveStatusFile uses RLock, fine for new tracker
			return fmt.Errorf("failed to create initial status file at %s: %w", statusFilePath, err)
		}
		logger.Printf("Initialized empty status file at %s", statusFilePath)
		return nil
	}

	// Read the status file (re-read using tracker's Load method for consistency)
	if err := statusTracker.LoadStatusFile(); err != nil {
		// LoadStatusFile logs errors and attempts to start fresh if unmarshalling fails.
		// If it returns an error here, it's likely a read error, not unmarshal.
		return fmt.Errorf("failed to load status data from %s: %w", statusFilePath, err)
	}

	// Use the data from the loaded statusTracker instance
	fmt.Println("=== Unicorn Photos Snapshot Status ===")
	fmt.Printf("Daemon running since: %s\n", statusTracker.StartTime.Format(time.RFC3339))
	fmt.Printf("Last status update: %s\n", statusTracker.LastUpdateTime.Format(time.RFC3339))
	fmt.Println()

	fmt.Printf("Total chains configured: %d\n", statusTracker.TotalChains)
	fmt.Printf("Chains actively snapshotting: %d\n", statusTracker.ActiveChains)
	fmt.Printf("Chains completed (idle): %d\n", statusTracker.CompletedChains)
	fmt.Printf("Chains pending processing: %d\n", statusTracker.PendingChains)
	fmt.Printf("Chains with errors: %d\n", statusTracker.ChainsWithErrors)
	fmt.Printf("Total successful snapshots: %d\n", statusTracker.TotalSnapshots)
	fmt.Println()

	if statusTracker.TotalChains == 0 {
		fmt.Println("No chains have been processed or registered yet.")
		fmt.Println("If the daemon is running, it might still be initializing or loading chains.")
		fmt.Println("Check daemon logs for more details.")
		return nil
	}

	var chainStatuses []ChainStatus
	// Access ChainStatuses via RLock if not using statusTracker directly
	statusTracker.mu.RLock()
	for _, cs := range statusTracker.ChainStatuses {
		chainStatuses = append(chainStatuses, cs)
	}
	statusTracker.mu.RUnlock()

	sort.Slice(chainStatuses, func(i, j int) bool {
		return chainStatuses[i].ChainID < chainStatuses[j].ChainID // Sort by RegistryName/key
	})

	fmt.Println("Chain Status Details:")
	fmt.Printf("%-20s %-30s %-12s %-12s %-12s %s\n", "CHAIN KEY", "NAME", "STATUS", "SNAPSHOTS", "LAST HEIGHT", "LAST ERROR")
	fmt.Println(strings.Repeat("-", 120)) // Adjusted width

	for _, cs := range chainStatuses {
		errorMsg := cs.LastError
		if len(errorMsg) > 40 { // Truncate long error messages for table view
			errorMsg = errorMsg[:37] + "..."
		}
		fmt.Printf("%-20s %-30s %-12s %-12d %-12d %s\n",
			cs.ChainID,                  // This is the registry name (key)
			truncateString(cs.Name, 28), // Pretty name
			cs.Status,
			cs.SnapshotCount,
			cs.LastSuccessfulHeight,
			errorMsg,
		)

		if cs.Status == "active" && cs.LastProgress != nil {
			progress := cs.LastProgress
			moduleStatus := progress.CurrentModule
			if progress.TotalModules > 0 { // Avoid division by zero if not set
				moduleStatus = fmt.Sprintf("%s (%d/%d)", progress.CurrentModule, len(progress.CompletedModules), progress.TotalModules)
			}
			fmt.Printf("  └─ Snapshotting H:%d: %d%% - Module: %s (Since: %s)\n",
				progress.Height,
				progress.PercentComplete,
				moduleStatus,
				formatTimeSince(progress.StartTime),
			)
		}
	}

	if statusTracker.ChainsWithErrors > 0 {
		fmt.Println("\nFull Error Details for Chains with Errors:")
		for _, cs := range chainStatuses {
			if cs.Status == "error" && cs.LastError != "" {
				fmt.Printf("  %s (%s):\n    Error: %s\n",
					cs.ChainID,
					cs.Name,
					cs.LastError,
				)
				if cs.LastProgress != nil && cs.LastProgress.Height > 0 {
					fmt.Printf("    Attempted Height: %d\n", cs.LastProgress.Height)
				}
			}
		}
	}

	return nil
}

// Helper function to truncate strings (remains the same)
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen < 3 {
		return s[:maxLen]
	} // Handle very small maxLen
	return s[:maxLen-3] + "..."
}

// Helper function to format time since nicely (remains the same)
func formatTimeSince(t time.Time) string {
	if t.IsZero() {
		return "N/A"
	}
	duration := time.Since(t)
	if duration < 0 {
		duration = -duration
	} // Handle if t is in future slightly

	if duration < time.Minute {
		return fmt.Sprintf("%ds ago", int(duration.Seconds()))
	} else if duration < time.Hour {
		return fmt.Sprintf("%dm ago", int(duration.Minutes()))
	} else if duration < 24*time.Hour {
		return fmt.Sprintf("%.1fh ago", duration.Hours())
	}
	return fmt.Sprintf("%.1fd ago", duration.Hours()/24)
}
