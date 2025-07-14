package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	cometlog "github.com/cometbft/cometbft/v2/libs/log"
	rpcserver "github.com/cometbft/cometbft/v2/rpc/jsonrpc/server"
	"github.com/ehsanranjbar/cometdump"
	"github.com/lmittmann/tint"
	"github.com/spf13/cobra"
)

const (
	defaultDataDir = "./blockchain-data"
	defaultAddr    = "0.0.0.0:8000"
	defaultRemote  = "http://localhost:26657/"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "cometdump",
		Short: "Efficiently store and serve CometBFT blockchain data",
		Long:  "CometDump is a tool for syncing, serving, and querying CometBFT blockchain data.",
	}

	rootCmd.AddCommand(syncCmd(), serveCmd(), blockCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func syncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync blocks and block results from CometBFT nodes to local storage",
		Run: func(cmd *cobra.Command, args []string) {
			dataDir, _ := cmd.Flags().GetString("data-dir")
			remote, _ := cmd.Flags().GetString("remote")
			height, _ := cmd.Flags().GetInt64("height")
			fetchSize, _ := cmd.Flags().GetInt("fetch-size")
			expandRemotes, _ := cmd.Flags().GetBool("expand-remotes")
			useLatestVersion, _ := cmd.Flags().GetBool("use-latest-version")
			chunkSize, _ := cmd.Flags().GetInt("chunk-size")
			numWorkers, _ := cmd.Flags().GetInt("workers")
			verbose, _ := cmd.Flags().GetBool("verbose")

			logLevel := slog.LevelInfo
			if verbose {
				logLevel = slog.LevelDebug
			}
			logger := slog.New(tint.NewHandler(os.Stdout, &tint.Options{
				Level:      logLevel,
				TimeFormat: time.RFC3339,
			}))

			// Open store
			store, err := cometdump.Open(dataDir)
			if err != nil {
				logger.Error("Failed to open store", "error", err)
				os.Exit(1)
			}

			// Configure sync
			config := cometdump.DefaultSyncConfig(remote).
				WithChunkSize(chunkSize).
				WithHeight(height).
				WithFetchSize(fetchSize).
				WithNumWorkers(numWorkers).
				WithExpandRemotes(expandRemotes).
				WithUseLatestVersion(useLatestVersion).
				WithLogger(logger.With("module", "sync"))

			// Start sync
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Handle interrupt signals
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigChan
				logger.Info("Received interrupt signal, stopping sync...")
				cancel()
			}()

			logger.Info("Starting sync", "remote", remote, "data_dir", dataDir)
			err = store.Sync(ctx, config)
			if err != nil {
				if ctx.Err() != nil {
					logger.Info("Sync cancelled by user")
				} else {
					logger.Error("Sync failed", "error", err)
					os.Exit(1)
				}
			} else {
				logger.Info("Sync completed successfully")
			}
		},
	}

	cmd.Flags().String("data-dir", defaultDataDir, "Directory to store blockchain data")
	cmd.Flags().String("remote", defaultRemote, "Remote CometBFT node URL")
	cmd.Flags().Int64("height", 0, "Sync up to this height (0 for latest)")
	cmd.Flags().Int("fetch-size", 100, "Number of blocks to fetch in each request")
	cmd.Flags().Bool("expand-remotes", true, "Discover additional nodes from the network")
	cmd.Flags().Bool("use-latest-version", true, "Use nodes with the latest application version")
	cmd.Flags().Int("chunk-size", 10000, "Number of blocks per chunk file")
	cmd.Flags().Int("workers", 4, "Number of concurrent workers")
	cmd.Flags().Bool("verbose", false, "Enable verbose logging")

	return cmd
}

func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start an RPC server to serve stored blockchain data",
		Run: func(cmd *cobra.Command, args []string) {
			dataDir, _ := cmd.Flags().GetString("data-dir")
			addr, _ := cmd.Flags().GetString("addr")
			verbose, _ := cmd.Flags().GetBool("verbose")

			logLevel := slog.LevelInfo
			if verbose {
				logLevel = slog.LevelDebug
			}
			logger := slog.New(tint.NewHandler(os.Stdout, &tint.Options{
				Level:      logLevel,
				TimeFormat: time.RFC3339,
			}))

			// Open store
			store, err := cometdump.Open(dataDir)
			if err != nil {
				logger.Error("Failed to open store", "error", err)
				os.Exit(1)
			}

			// Create RPC server
			cometLogger := cometlog.NewLogger(os.Stdout)
			server := cometdump.NewRPCServer(store)
			mux := http.NewServeMux()
			rpcserver.RegisterRPCFuncs(mux, server.GetRoutes(), cometLogger)

			// Start server
			listener, err := rpcserver.Listen("tcp://"+addr, 0)
			if err != nil {
				logger.Error("Failed to create listener", "error", err)
				os.Exit(1)
			}

			logger.Info("Starting RPC server", "address", "http://"+addr, "data_dir", dataDir)
			logger.Info("Available endpoints:",
				"block", "GET /block?height=N",
				"block_results", "GET /block_results?height=N",
				"blockchain", "GET /blockchain?minHeight=N&maxHeight=M",
				"header", "GET /header?height=N")

			// Handle interrupt signals
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigChan
				logger.Info("Received interrupt signal, shutting down server...")
				os.Exit(0)
			}()

			// Start serving
			err = rpcserver.Serve(listener, mux, cometLogger, rpcserver.DefaultConfig())
			if err != nil {
				logger.Error("Server error", "error", err)
				os.Exit(1)
			}
		},
	}

	cmd.Flags().String("data-dir", defaultDataDir, "Directory containing blockchain data")
	cmd.Flags().String("addr", defaultAddr, "Address to bind the RPC server")
	cmd.Flags().Bool("verbose", false, "Enable verbose logging")

	return cmd
}

func blockCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "block",
		Short: "Retrieve and display a specific block",
		Run: func(cmd *cobra.Command, args []string) {
			dataDir, _ := cmd.Flags().GetString("data-dir")
			height, _ := cmd.Flags().GetInt64("height")
			verbose, _ := cmd.Flags().GetBool("verbose")

			if height <= 0 {
				fmt.Fprintf(os.Stderr, "Error: --height is required and must be greater than 0\n")
				cmd.Usage()
				os.Exit(1)
			}

			logLevel := slog.LevelInfo
			if verbose {
				logLevel = slog.LevelDebug
			}
			logger := slog.New(tint.NewHandler(os.Stdout, &tint.Options{
				Level:      logLevel,
				TimeFormat: time.RFC3339,
			}))

			// Open store
			store, err := cometdump.Open(dataDir)
			if err != nil {
				logger.Error("Failed to open store", "error", err)
				os.Exit(1)
			}

			// Get block
			block, err := store.BlockAt(height)
			if err != nil {
				logger.Error("Failed to retrieve block", "height", height, "error", err)
				os.Exit(1)
			}

			// Display block based on format
			fmt.Printf("Block Height: %d\n", block.Block.Height)
			fmt.Printf("Block Hash: %s\n", block.Block.Hash())
			fmt.Printf("Time: %s\n", block.Block.Time.Format(time.RFC3339))
			fmt.Printf("Proposer: %X\n", block.Block.ProposerAddress)
			fmt.Printf("Transactions: %d\n", len(block.Block.Data.Txs))
		},
	}

	cmd.Flags().String("data-dir", defaultDataDir, "Directory containing blockchain data")
	cmd.Flags().Int64("height", 0, "Block height to retrieve (required)")
	cmd.Flags().Bool("verbose", false, "Enable verbose logging")

	return cmd
}
