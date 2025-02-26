package main

// takeSnapshot creates a snapshot at the specified height and returns the snapshot directory path
func takeSnapshot(height int64) error {
	logger.Printf("Taking snapshot at height %d", height)
	return takeSnapshotCore(height)
}
