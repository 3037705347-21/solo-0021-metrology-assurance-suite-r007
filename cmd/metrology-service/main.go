package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/solo-0021-metrology-assurance-suite/internal/bootstrap"
)

const shutdownTimeout = 5 * time.Second

func main() {
	addr := flag.String("addr", "127.0.0.1:18121", "HTTP listen address")
	flag.Parse()

	logger := log.New(os.Stderr, "", log.LstdFlags)
	if err := run(*addr, logger); err != nil {
		logger.Print(err)
		os.Exit(1)
	}
}

func run(addr string, logger *log.Logger) error {
	// Bind synchronously: a bind failure is exposed before any "ready" signal,
	// so callers never confuse a failed start with a running service.
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	server := &http.Server{
		Handler:           bootstrap.NewHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()
	logger.Printf("metrology assurance suite listening on %s", listener.Addr())

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			logger.Print("server stopped")
			return nil
		}
		return fmt.Errorf("server stopped with error: %w", err)
	case <-signalCtx.Done():
		logger.Printf("termination signal (%v) received, shutting down", signalCtx.Err())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)

	// Serve returns once Shutdown closes the listener; drain it so the
	// shutdown outcome is the single, repeatable result callers observe.
	serveError := <-serveErr
	if serveError != nil && !errors.Is(serveError, http.ErrServerClosed) {
		return fmt.Errorf("graceful shutdown interrupted: %w", serveError)
	}
	if shutdownErr != nil {
		// Shutdown exceeded the deadline: release remaining resources forcibly.
		if closeErr := server.Close(); closeErr != nil {
			return fmt.Errorf("graceful shutdown did not complete: %w; forced close: %v", shutdownErr, closeErr)
		}
		return fmt.Errorf("graceful shutdown did not complete: %w", shutdownErr)
	}

	logger.Print("shutdown complete")
	return nil
}
