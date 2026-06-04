package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	ordersrepo "github.com/odestroyero/debezium-demo-lab/internal/orders/repository/mongo"
	orderssvc "github.com/odestroyero/debezium-demo-lab/internal/orders/service"
	httpapi "github.com/odestroyero/debezium-demo-lab/internal/orders/transport/http"
	"github.com/odestroyero/debezium-demo-lab/internal/platform/env"
	"github.com/odestroyero/debezium-demo-lab/internal/platform/mongodb"
)

const (
	defaultMongoURI = "mongodb://localhost:27017/?replicaSet=rs0"
	defaultDatabase = "orders"
	defaultHTTPAddr = ":8080"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mongoURI := env.String("MONGODB_URI", defaultMongoURI)
	database := env.String("MONGODB_DATABASE", defaultDatabase)
	httpAddr := env.String("HTTP_ADDR", defaultHTTPAddr)

	client, err := mongodb.Connect(ctx, mongoURI)
	if err != nil {
		log.Fatalf("connect mongodb: %v", err)
	}
	defer func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Disconnect(disconnectCtx)
	}()

	repository := ordersrepo.New(client, database)
	service := orderssvc.New(repository)
	if err := service.EnsureReady(ctx); err != nil {
		log.Fatalf("ensure indexes: %v", err)
	}

	mux := http.NewServeMux()
	httpapi.New(service).Register(mux)

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("orders api listening on %s", httpAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %v", err)
	}
}
