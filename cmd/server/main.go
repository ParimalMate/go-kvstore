package main

import (
	"context"
	"errors"
	"flag"
	"kvstore/internal/api"
	"kvstore/internal/store"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	port := flag.String("port", "8080", "HTTP server port")
	dataFile := flag.String("data", "kvlog", "WAL file path")
	id := flag.String("id", "n1", "unique node ID")
	peers := flag.String("peers", "", "comma-separated peer addresses")
	w := flag.Int("w", 2, "write quorum size, including the local node")
	r := flag.Int("r", 2, "read quorum size, including the local node")
	flag.Parse()

	if strings.TrimSpace(*id) == "" {
		log.Fatal("node ID cannot be empty")
	}

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

	selfPort := *port

	for _, peer := range peerList {
		host, peerPort, err := net.SplitHostPort(peer)
		if err != nil {
			logger.Fatalf("invalid peer address %q: %v", peer, err)
		}

		if peerPort != selfPort {
			continue
		}

		ip := net.ParseIP(host)

		isLocalhost := host == "localhost" || host == ""
		isLoopback := ip != nil && ip.IsLoopback()
		isWildcard := ip != nil && ip.IsUnspecified()

		if isLocalhost || isLoopback || isWildcard {
			logger.Fatalf("node cannot be its own peer: %q", peer)
		}
	}

	n := len(peerList) + 1
	if *w < 1 || *w > n || *r < 1 || *r > n {
		logger.Fatalf("invalid quorum configuration: W=%d R=%d N=%d; W and R must each be between 1 and N", *w, *r, n)
	}
	if *w+*r <= n {
		logger.Fatalf("invalid quorum configuration: W=%d R=%d N=%d; W+R must be greater than N", *w, *r, n)
	}

	kvStore := store.NewStore(*dataFile)
	defer kvStore.Close()

	handlers := api.NewHandler(kvStore, logger, peerList, *w, *r, *id)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", handlers.GetHandler)
	mux.HandleFunc("PUT /kv/{key}", handlers.PutHandler)
	mux.HandleFunc("GET /internal/kv/{key}", handlers.InternalGetHandler)
	mux.HandleFunc("PUT /internal/kv/{key}", handlers.InternalPutHandler)

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
