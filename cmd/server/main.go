package main

import (
	"context"
	"errors"
	"flag"
	"kvstore/internal/api"
	"kvstore/internal/gossip"
	"kvstore/internal/store"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	generation := time.Now().UnixNano()
	cfg := defaultConfig()
	var rawPeers string
	flag.StringVar(&cfg.Port, "port", cfg.Port, "HTTP server port")
	flag.StringVar(&cfg.DataFile, "data", cfg.DataFile, "WAL file path")
	flag.StringVar(&cfg.ID, "id", cfg.ID, "unique node ID")
	flag.StringVar(&rawPeers, "peers", "", "comma-separated peer addresses")
	flag.IntVar(&cfg.W, "w", cfg.W, "write quorum size, including the local node")
	flag.IntVar(&cfg.R, "r", cfg.R, "read quorum size, including the local node")
	flag.DurationVar(&cfg.GossipInterval, "gossip-interval", cfg.GossipInterval, "gossip round interval (10ms to 1m)")
	flag.DurationVar(&cfg.SuspectAfter, "suspect-after", cfg.SuspectAfter, "suspect threshold (at least 3 gossip intervals)")
	flag.DurationVar(&cfg.DeadAfter, "dead-after", cfg.DeadAfter, "dead threshold (greater than suspect threshold)")
	flag.Parse()
	cfg.Peers = parsePeers(rawPeers)
	if err := validateConfig(cfg); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	logger := log.New(os.Stdout, "["+cfg.ID+"] ", log.LstdFlags|log.Lmicroseconds)

	kvStore := store.NewStore(cfg.DataFile)
	defer kvStore.Close()
	gossipTable, err := gossip.NewTable(cfg.ID, generation, gossip.Timing{
		Interval: cfg.GossipInterval, SuspectAfter: cfg.SuspectAfter, DeadAfter: cfg.DeadAfter,
	}, time.Now)
	if err != nil {
		logger.Fatalf("gossip configuration failed: %v", err)
	}
	gossipService, err := gossip.NewService(gossipTable, logger, cfg.Peers)
	if err != nil {
		logger.Fatalf("gossip service failed: %v", err)
	}
	defer gossipService.Stop()

	handlers := api.NewHandler(kvStore, logger, cfg.Peers, cfg.W, cfg.R, cfg.ID)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", handlers.GetHandler)
	mux.HandleFunc("PUT /kv/{key}", handlers.PutHandler)
	mux.HandleFunc("GET /internal/kv/{key}", handlers.InternalGetHandler)
	mux.HandleFunc("PUT /internal/kv/{key}", handlers.InternalPutHandler)
	gossipService.RegisterRoutes(mux)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	errCh := make(chan error, 1)

	go func() {
		errCh <- server.ListenAndServe()
	}()
	gossipService.Start(ctx)

	logger.Printf("listening on port %s", cfg.Port)

	select {
	case <-ctx.Done():
		logger.Println("Shutdown signal received")

	case err := <-errCh:
		gossipService.Stop()
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("HTTP server failed: %v", err)
		}
		return
	}

	gossipService.Stop()
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	err = server.Shutdown(shutdownCtx)
	if err != nil {
		logger.Printf("Graceful shutdown timed out: %v", err)
		if closeErr := server.Close(); closeErr != nil {
			logger.Printf("Server close failed: %v", closeErr)
		}
	}

	logger.Println("Server shut down cleanly")
}
