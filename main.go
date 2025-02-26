package main

import (
	"context"
	"flag"
	"log"
)

func main() {
	daemonMode := flag.Bool("daemon", false, "Run in daemon mode")
	flag.Parse()

	if *daemonMode {
		ctx := context.Background()
		if err := runDaemon(ctx); err != nil {
			log.Fatalf("Daemon error: %v", err)
		}
		return
	}

	height, err := getLatestBlockHeight()
	if err != nil {
		log.Fatalf("Failed to get latest block height: %v", err)
	}

	if err := takeSnapshot(height); err != nil {
		log.Fatalf("Failed to take snapshot: %v", err)
	}
}
