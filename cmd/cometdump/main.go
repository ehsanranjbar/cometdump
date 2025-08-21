package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	abcitypes "github.com/cometbft/cometbft/v2/abci/types"
	cometjson "github.com/cometbft/cometbft/v2/libs/json"
	cometlog "github.com/cometbft/cometbft/v2/libs/log"
	rpcserver "github.com/cometbft/cometbft/v2/rpc/jsonrpc/server"
	"github.com/ehsanranjbar/cometdump"
	"github.com/lmittmann/tint"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vbauerster/mpb/v8"
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

	rootCmd.AddCommand(syncCmd(), serveCmd(), blockCmd(), blockResultsCmd(), normalizeCmd())

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
			base, _ := cmd.Flags().GetInt64("base")
			target, _ := cmd.Flags().GetInt64("target")
			fetchSize, _ := cmd.Flags().GetInt("fetch-size")
			expandRemotes, _ := cmd.Flags().GetBool("expand-remotes")
			useLatestVersion, _ := cmd.Flags().GetBool("use-latest-version")
			chunkSize, _ := cmd.Flags().GetInt("chunk-size")
			numWorkers, _ := cmd.Flags().GetInt("workers")
			verbose, _ := cmd.Flags().GetBool("verbose")
			noProgress, _ := cmd.Flags().GetBool("no-progress")

			logger := getLogger(cmd.Flags())
			var pbar *mpb.Progress
			if !noProgress {
				pbar = mpb.New()
				logLevel := slog.LevelInfo
				if verbose {
					logLevel = slog.LevelDebug
				}
				logger = slog.New(tint.NewHandler(pbar, &tint.Options{
					Level:      logLevel,
					TimeFormat: time.RFC3339,
				}))
			}

			// Open store
			store, err := cometdump.Open(cometdump.DefaultOpenOptions(dataDir).
				WithLogger(logger))
			if err != nil {
				logger.Error("Failed to open store", "error", err)
				os.Exit(1)
			}

			// Configure sync
			config := cometdump.DefaultSyncOptions(remote).
				WithChunkSize(chunkSize).
				WithBaseHeight(base).
				WithTargetHeight(target).
				WithFetchSize(fetchSize).
				WithNumWorkers(numWorkers).
				WithExpandRemotes(expandRemotes).
				WithUseLatestVersion(useLatestVersion).
				WithProgressBar(pbar).
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
	cmd.Flags().Int64("base", 0, "Base height to start syncing from (0 for latest)")
	cmd.Flags().Int64("target", 0, "Sync up to this height (0 for latest)")
	cmd.Flags().Int("fetch-size", 100, "Number of blocks to fetch in each request")
	cmd.Flags().Bool("expand-remotes", true, "Discover additional nodes from the network")
	cmd.Flags().Bool("use-latest-version", true, "Use nodes with the latest application version")
	cmd.Flags().Int("chunk-size", 10000, "Number of blocks per chunk file")
	cmd.Flags().Int("workers", 4, "Number of concurrent workers")
	cmd.Flags().Bool("verbose", false, "Enable verbose logging")
	cmd.Flags().Bool("no-progress", false, "Show progress bar during sync")

	return cmd
}

func getLogger(flags *pflag.FlagSet) *slog.Logger {
	verbose, _ := flags.GetBool("verbose")
	logLevel := slog.LevelInfo
	if verbose {
		logLevel = slog.LevelDebug
	}
	return slog.New(tint.NewHandler(os.Stdout, &tint.Options{
		Level:      logLevel,
		TimeFormat: time.RFC3339,
	}))
}

func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start an RPC server to serve stored blockchain data",
		Run: func(cmd *cobra.Command, args []string) {
			dataDir, _ := cmd.Flags().GetString("data-dir")
			addr, _ := cmd.Flags().GetString("addr")

			logger := getLogger(cmd.Flags())

			// Open store
			store, err := cometdump.Open(cometdump.DefaultOpenOptions(dataDir).
				WithLogger(logger))
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
			outputFormat, _ := cmd.Flags().GetString("output")

			if height <= 0 {
				fmt.Fprintf(os.Stderr, "Error: --height is required and must be greater than 0\n")
				cmd.Usage()
				os.Exit(1)
			}

			logger := getLogger(cmd.Flags())

			// Open store
			store, err := cometdump.Open(cometdump.DefaultOpenOptions(dataDir).
				WithLogger(logger))
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
			if outputFormat == "json" {
				bz, err := cometjson.Marshal(block.Block)
				if err != nil {
					logger.Error("Failed to marshal block to JSON", "error", err)
					os.Exit(1)
				}
				fmt.Println(string(bz))
			} else {
				fmt.Printf("Block Height: %d\n", block.Block.Height)
				fmt.Printf("Block Hash: %s\n", block.Block.Hash())
				fmt.Printf("Time: %s\n", block.Block.Time.Format(time.RFC3339))
				fmt.Printf("Proposer: %X\n", block.Block.ProposerAddress)
				fmt.Printf("Transactions: %d\n", len(block.Block.Data.Txs))
			}
		},
	}

	cmd.Flags().String("data-dir", defaultDataDir, "Directory containing blockchain data")
	cmd.Flags().Int64("height", 0, "Block height to retrieve (required)")
	cmd.Flags().Bool("verbose", false, "Enable verbose logging")
	cmd.Flags().String("output", "text", "Output format: text or json")

	return cmd
}

func blockResultsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "block-results",
		Short: "Retrieve and display the block results of a specific height",
		Run: func(cmd *cobra.Command, args []string) {
			dataDir, _ := cmd.Flags().GetString("data-dir")
			height, _ := cmd.Flags().GetInt64("height")
			outputFormat, _ := cmd.Flags().GetString("output")

			if height <= 0 {
				fmt.Fprintf(os.Stderr, "Error: --height is required and must be greater than 0\n")
				cmd.Usage()
				os.Exit(1)
			}

			logger := getLogger(cmd.Flags())

			// Open store
			store, err := cometdump.Open(cometdump.DefaultOpenOptions(dataDir).
				WithLogger(logger))
			if err != nil {
				logger.Error("Failed to open store", "error", err)
				os.Exit(1)
			}

			// Get block results
			block, err := store.BlockAt(height)
			if err != nil {
				logger.Error("Failed to retrieve block results", "height", height, "error", err)
				os.Exit(1)
			}

			// Display block results based on format
			if outputFormat == "json" {
				bz, err := cometjson.Marshal(block.ToResultBlockResults())
				if err != nil {
					logger.Error("Failed to marshal block results to JSON", "error", err)
					os.Exit(1)
				}
				fmt.Println(string(bz))
			} else {
				fmt.Printf("Block Height: %d\n", height)
				fmt.Println("Txs:")
				if len(block.TxResults) == 0 {
					fmt.Println("    No transactions")
				}
				for _, tx := range block.TxResults {
					fmt.Printf("  - Code: %d\n", tx.Code)
					fmt.Printf("    Data: %s\n", base64.StdEncoding.EncodeToString(tx.Data))
					fmt.Printf("    GasWanted: %d\n", tx.GasWanted)
					fmt.Printf("    GasUsed: %d\n", tx.GasUsed)
					fmt.Println("    Events:")
					printEvents("      ", tx.Events)
				}
				fmt.Println("FinalizeBlockEvents:")
				printEvents("    ", block.FinalizeBlockEvents)
			}
		},
	}

	cmd.Flags().String("data-dir", defaultDataDir, "Directory containing blockchain data")
	cmd.Flags().Int64("height", 0, "Block height to retrieve results for (required)")
	cmd.Flags().Bool("verbose", false, "Enable verbose logging")
	cmd.Flags().String("output", "text", "Output format: text or json")

	return cmd
}

func printEvents(indent string, events []abcitypes.Event) {
	if len(events) == 0 {
		fmt.Printf("%sNo events\n", indent)
		return
	}
	for _, event := range events {
		fmt.Printf("%s- Type: %s\n", indent, event.Type)
		for _, attr := range event.Attributes {
			key := string(attr.Key)
			value := string(attr.Value)
			fmt.Printf("%s  - %s: %s\n", indent, key, value)
		}
		if len(event.Attributes) == 0 {
			fmt.Printf("%s  - No attributes\n", indent)
		}
	}
}

func normalizeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "normalize",
		Short: "Normalize the chunks in the store to a specified size",
		Run: func(cmd *cobra.Command, args []string) {
			dataDir, _ := cmd.Flags().GetString("data-dir")
			chunkSize, _ := cmd.Flags().GetInt64("chunk-size")
			verbose, _ := cmd.Flags().GetBool("verbose")

			if chunkSize <= 0 {
				fmt.Fprintf(os.Stderr, "Error: --chunk-size must be greater than 0\n")
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
			store, err := cometdump.Open(cometdump.DefaultOpenOptions(dataDir).
				WithLogger(logger))
			if err != nil {
				logger.Error("Failed to open store", "error", err)
				os.Exit(1)
			}

			// Normalize chunks
			logger.Info("Normalizing chunks", "chunk_size", chunkSize)
			if err := store.Normalize(chunkSize); err != nil {
				logger.Error("Failed to normalize chunks", "error", err)
				os.Exit(1)
			}
			logger.Info("Store normalized successfully", "chunk_size", chunkSize)
		},
	}

	cmd.Flags().String("data-dir", defaultDataDir, "Directory containing blockchain data")
	cmd.Flags().Int64("chunk-size", 10000, "Size of each chunk after normalization")
	cmd.Flags().Bool("verbose", false, "Enable verbose logging")

	return cmd
}
