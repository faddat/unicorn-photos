package main

// takeSnapshot creates a snapshot at the specified height and returns the snapshot directory path
func takeSnapshot(height int64) error {
	return takeSnapshotCore(height)
}
