# unicorn photos

Code and output for a bespoke snapshot utility for Unicorn and Memes that automatically takes snapshots every 4 hours and stores them on IPFS.

## Prerequisites

1. Go 1.21 or later
2. IPFS daemon running locally (install from https://docs.ipfs.tech/install/)

## Installation

1. Install the binary:
```bash
go install ./...
```

2. Install the systemd service:
```bash
sudo cp unicorn-photos.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable unicorn-photos
sudo systemctl start unicorn-photos
```

## Usage

### Daemon Mode (Recommended)
The daemon will automatically:
- Take snapshots every 4 hours
- Store snapshots on IPFS
- Update the README with latest IPFS CIDs

To start the daemon:
```bash
sudo systemctl start unicorn-photos
```

### Manual Mode
To take a single snapshot:
```bash
unicorn-photos
```

## IPFS Snapshots

The snapshots are stored on IPFS and can be accessed using any IPFS gateway.

Latest snapshot index CID: (Will be updated by daemon)

### Recent Snapshots

(Will be updated by daemon)

## Current Features

- Automatic snapshots every 4 hours
- IPFS integration for decentralized snapshot storage
- Cosmos-SDK v0.50.x compatible genesis.json generation

## Purpose

This frees the unicorn.

Latest snapshot from block height: (Will be updated by daemon)

## Planned features

* snapshot a specific block height
* snapshot a specific solana block height
* 
