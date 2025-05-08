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
	statusFilePath := filepath.Join(baseDir, "status.json")

	// Check if status file exists
	if _, err := os.Stat(statusFilePath); os.IsNotExist(err) {
		return fmt.Errorf("status file not found at %s - daemon may not be running or has not created any snapshots yet", statusFilePath)
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
