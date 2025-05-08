package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Command to display snapshot status information
func cmdStatus() error {
	baseDir := "snapshots"

	// Create the snapshots directory if it doesn't exist
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		if err := os.MkdirAll(baseDir, 0755); err != nil {
			return fmt.Errorf("failed to create snapshots directory: %w", err)
		}
		fmt.Println("=== Unicorn Photos - Status ===")
		fmt.Println("No snapshots have been taken yet.")
		fmt.Println("\nTo start the snapshot daemon, run:")
		fmt.Println("  unicorn-photos run")
		fmt.Println("\nThis will initialize the status tracking system and begin snapshotting chains.")
		return nil
	}

	statusFilePath := filepath.Join(baseDir, "status.json")

	// Check if status file exists
	if _, err := os.Stat(statusFilePath); os.IsNotExist(err) {
		// Create a default status tracker with a helpful message
		fmt.Println("=== Unicorn Photos - Status ===")
		fmt.Println("Snapshot daemon has not been started yet.")
		fmt.Println("\nTo start the snapshot daemon, run:")
		fmt.Println("  unicorn-photos run")
		fmt.Println("\nThis will initialize the status tracking system and begin snapshotting chains.")

		// Initialize a default status file
		statusTracker := GetStatusTracker()
		if err := statusTracker.SaveStatusFile(); err != nil {
			return fmt.Errorf("failed to create initial status file: %w", err)
		}
		return nil
	}

	// Read the status file
	data, err := os.ReadFile(statusFilePath)
	if err != nil {
		return fmt.Errorf("failed to read status file: %w", err)
	}

	// Parse the status data
	var status StatusTracker
	if err := json.Unmarshal(data, &status); err != nil {
		return fmt.Errorf("failed to parse status file: %w", err)
	}

	// Print summary information
	fmt.Println("=== Unicorn Photos Snapshot Status ===")
	fmt.Printf("Running since: %s\n", status.StartTime.Format(time.RFC3339))
	fmt.Printf("Last updated: %s\n", status.LastUpdateTime.Format(time.RFC3339))
	fmt.Println()

	fmt.Printf("Total chains:        %d\n", status.TotalChains)
	fmt.Printf("Active chains:       %d\n", status.ActiveChains)
	fmt.Printf("Completed chains:    %d\n", status.CompletedChains)
	fmt.Printf("Pending chains:      %d\n", status.PendingChains)
	fmt.Printf("Chains with errors:  %d\n", status.ChainsWithErrors)
	fmt.Printf("Total snapshots:     %d\n", status.TotalSnapshots)
	fmt.Println()

	// If there are no chains yet, display a helpful message
	if status.TotalChains == 0 {
		fmt.Println("No chains have been configured for snapshotting yet.")
		fmt.Println("Make sure your config.toml contains valid chain configurations.")
		fmt.Println("The daemon will automatically discover and register chains.")
		return nil
	}

	// Convert map to sortable slice
	var chainStatuses []ChainStatus
	for _, status := range status.ChainStatuses {
		chainStatuses = append(chainStatuses, status)
	}

	// Sort by chain ID
	sort.Slice(chainStatuses, func(i, j int) bool {
		return chainStatuses[i].ChainID < chainStatuses[j].ChainID
	})

	// Output table of chains
	fmt.Println("Chain Status:")
	fmt.Printf("%-20s %-30s %-12s %-12s %s\n", "CHAIN ID", "NAME", "STATUS", "SNAPSHOTS", "LAST HEIGHT")
	fmt.Println(strings.Repeat("-", 100))

	for _, chainStatus := range chainStatuses {
		fmt.Printf("%-20s %-30s %-12s %-12d %d\n",
			chainStatus.ChainID,
			truncateString(chainStatus.Name, 30),
			chainStatus.Status,
			chainStatus.SnapshotCount,
			chainStatus.LastSuccessfulHeight,
		)

		// If there's a snapshot in progress, show progress details
		if chainStatus.Status == "active" && chainStatus.LastProgress != nil {
			progress := chainStatus.LastProgress
			fmt.Printf("  └─ %s: %d%% complete - Current module: %s (%d/%d modules completed)\n",
				formatTimeSince(progress.StartTime),
				progress.PercentComplete,
				progress.CurrentModule,
				len(progress.CompletedModules),
				progress.TotalModules)
		}
	}

	// Output error details if any
	if status.ChainsWithErrors > 0 {
		fmt.Println("\nError Details:")
		for _, chainStatus := range chainStatuses {
			if chainStatus.Status == "error" && chainStatus.LastError != "" {
				fmt.Printf("%s (%s): %s\n",
					chainStatus.ChainID,
					chainStatus.Name,
					chainStatus.LastError,
				)
			}
		}
	}

	return nil
}

// Helper function to truncate strings
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// Helper function to format time since nicely
func formatTimeSince(t time.Time) string {
	duration := time.Since(t)
	if duration < time.Minute {
		return fmt.Sprintf("%d seconds", int(duration.Seconds()))
	} else if duration < time.Hour {
		return fmt.Sprintf("%d minutes", int(duration.Minutes()))
	} else if duration < 24*time.Hour {
		return fmt.Sprintf("%.1f hours", duration.Hours())
	}
	return fmt.Sprintf("%.1f days", duration.Hours()/24)
}
