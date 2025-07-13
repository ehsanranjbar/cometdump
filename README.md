# CometDump

[![PkgGoDev](https://pkg.go.dev/badge/github.com/ehsanranjbar/cometdump.svg)](https://pkg.go.dev/github.com/ehsanranjbar/cometdump)

A Go library and CLI tool for efficiently storing and retrieving CometBFT blockchain data. CometDump downloads blocks from CometBFT-based chains and stores them in compressed chunks for fast local access.

## Features

- 🚀 **Fast Block Retrieval**: Efficiently fetch blocks from multiple CometBFT nodes with concurrent workers
- 💾 **Compressed Storage**: Blocks are stored in compressed [msgpack](https://msgpack.org/index.html) format using [Brotli](https://github.com/google/brotli) compression
- 🔍 **Smart Node Discovery**: Automatically discover and select the best nodes from the network
- ⚡ **Iterator Interface**: Stream through blocks with Go's iterator pattern
- 🌐 **RPC Mirror**: Serve stored blockchain data via CometBFT-compatible JSON-RPC endpoints
- 🔧 **Configurable Sync**: Flexible sync configuration with version constraints and chunk sizing

## Installation

```bash
go get github.com/ehsanranjbar/cometdump
```

## Usage

### Example

This example demonstrates syncing blocks from a CometBFT node, iterating through stored blocks, and accessing specific blocks:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "log/slog"
    "os"

    "github.com/ehsanranjbar/cometdump"
)

func main() {
    // Open or create a store directory
    store, err := cometdump.Open("./blockchain-data")
    if err != nil {
        log.Fatal(err)
    }

    // Create a logger
    logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
        Level: slog.LevelInfo,
    }))
    // Configure sync options and sync blocks from CometBFT nodes
    config := cometdump.DefaultSyncConfig("https://cosmos-rpc.publicnode.com:443/").
        WithExpandRemotes(true).                    // Discover additional nodes
        WithUseLatestVersion(true).                 // Use nodes with latest version
        WithHeight(1000).                           // Sync up to block 1,000
        WithLogger(logger.With("module", "sync"))

    ctx := context.Background()
    err = store.Sync(ctx, config)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Sync completed!")

    // Iterate through all stored blocks using the Blocks method
    fmt.Println("\nIterating through blocks:")
    for block, err := range store.Blocks() {
        if err != nil {
            log.Printf("Error reading block: %v", err)
            continue
        }

        fmt.Printf("Block Height: %d, Hash: %s, Txs: %d\n",
            block.Block.Height,
            block.Block.Hash(),
            len(block.Block.Data.Txs))

        // Process first 10 blocks only for this example
        if block.Block.Height >= 10 {
            break
        }
    }
}
```

### RPC Server Example

CometDump includes an RPC server that provides CometBFT-compatible JSON-RPC endpoints for accessing stored blockchain data:

```go
package main

import (
    "log"
    "net/http"
    "os"

    cometlog "github.com/cometbft/cometbft/v2/libs/log"
    rpcserver "github.com/cometbft/cometbft/v2/rpc/jsonrpc/server"
    "github.com/ehsanranjbar/cometdump"
)

func main() {
    // Open the store
    store, err := cometdump.Open("./blockchain-data")
    if err != nil {
        log.Fatal(err)
    }

    // Create RPC server and register routes
    mux := http.NewServeMux()
    server := cometdump.NewRPCServer(store)
    logger := cometlog.NewLogger(os.Stdout)
    rpcserver.RegisterRPCFuncs(mux, server.GetRoutes(), logger)

    // Start the RPC server
    listener, err := rpcserver.Listen("tcp://localhost:8080", 0)
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Starting RPC server", "address", "http://localhost:8080")

    if err := rpcserver.Serve(listener, mux, logger, rpcserver.DefaultConfig()); err != nil {
        log.Fatal(err)
    }
}
```

The RPC server provides the following CometBFT-compatible endpoints:

- `GET /block?height=N` - Get block at specific height
- `GET /block_results?height=N` - Get block results (transaction results, events)
- `GET /blockchain?minHeight=N&maxHeight=M` - Get block headers in range
- `GET /header?height=N` - Get block header at specific height

## Requirements

- Go 1.23.5 or later
- Compatible with CometBFT v2.x nodes

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

## Related Projects

- [CometBFT](https://github.com/cometbft/cometbft) - The underlying consensus engine
- [Cosmos SDK](https://github.com/cosmos/cosmos-sdk) - Framework for building blockchain applications
