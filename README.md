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

## Snapshot 11794277

- Height: 11794277
- Time: 2025-02-20T09:39:31Z
- Path: snapshots/height_11794277
- Total Supply: 67940412438764700 uwunicorn (67940412438.764702 UNICORN)

## Snapshot 11795420

- Height: 11795420
- Time: 2025-02-20T10:36:39Z
- Path: snapshots/height_11795420
- Total Supply: 67724381388052786 uwunicorn (67724381388.052788 UNICORN)

## Snapshot 11796162

- Height: 11796162
- Time: 2025-02-20T11:34:27Z
- Path: snapshots/height_11796162
- Total Supply: 68226508905947145 uwunicorn (68226508905.947144 UNICORN)

## Snapshot 11855216

- Height: 11855216
- Time: 2025-02-22T12:45:05Z
- Path: snapshots/height_11855216
- Total Supply: 68061648767663797 uwunicorn (68061648767.663803 UNICORN)

## Snapshot 11909096

- Height: 11909096
- Time: 2025-02-25T04:32:30Z
- Path: snapshots/height_11909096
- Total Supply: 0 uwunicorn (0.000000 UNICORN)
