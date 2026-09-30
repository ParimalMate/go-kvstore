package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"kvstore/internal/api"
	"kvstore/internal/store"
)

func main() {
	port := flag.String("port", "8080", "HTTP server port")
	dataFile := flag.String("data", "kvlog", "WAL file path")
	id := flag.String("id", "n1", "unique node ID")
	peers := flag.String("peers", "", "comma-separated peer addresses")
	flag.Parse()

	logger := log.New(
		os.Stdout,
		"["+*id+"] ",
		log.LstdFlags|log.Lmicroseconds,
	)

	var peerList []string
	if *peers != "" {
		for _, peer := range strings.Split(*peers, ",") {
			peer = strings.TrimSpace(peer)
			if peer != "" {
				peerList = append(peerList, peer)
			}
		}
	}

	selfAddr := fmt.Sprintf("localhost:%s", *port)
	for _, peer := range peerList {
		if peer == selfAddr {
			logger.Fatalf(
				"node %s cannot list itself as a peer: %s",
				*id,
				peer,
			)
		}
	}

	kvStore := store.NewStore(*dataFile)
	defer kvStore.Close()

	handlers := api.NewHandler(kvStore, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", handlers.GetHandler)
	mux.HandleFunc("PUT /kv/{key}", handlers.PutHandler)

	server := &http.Server{
		Addr:    ":" + *port,
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

	logger.Printf("listening on port %s", *port)

	select {
	case <-ctx.Done():
		logger.Println("Shutdown signal received")

	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("HTTP server failed: %v", err)
		}
		return
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	err := server.Shutdown(shutdownCtx)
	if err != nil {
		logger.Printf("Graceful shutdown timed out: %v", err)
		if closeErr := server.Close(); closeErr != nil {
			logger.Printf("Server close failed: %v", closeErr)
		}
	}

	logger.Println("Server shut down cleanly")
}
