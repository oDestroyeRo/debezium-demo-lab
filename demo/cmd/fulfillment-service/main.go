package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	kafkaconsumer "github.com/odestroyero/debezium-demo-lab/internal/fulfillment/consumer/kafka"
	fulfillmentrepo "github.com/odestroyero/debezium-demo-lab/internal/fulfillment/repository/mongo"
	fulfillmentsvc "github.com/odestroyero/debezium-demo-lab/internal/fulfillment/service"
	"github.com/odestroyero/debezium-demo-lab/internal/platform/env"
	"github.com/odestroyero/debezium-demo-lab/internal/platform/mongodb"
)

const (
	defaultMongoURI = "mongodb://localhost:27017/?replicaSet=rs0"
	defaultDatabase = "fulfillment"
	defaultBrokers  = "localhost:9092"
	defaultTopic    = "orders.events.v1"
	defaultGroupID  = "fulfillment-service"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := mongodb.Connect(ctx, env.String("MONGODB_URI", defaultMongoURI))
	if err != nil {
		log.Fatalf("connect mongodb: %v", err)
	}
	defer func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Disconnect(disconnectCtx)
	}()

	repository := fulfillmentrepo.New(client, env.String("FULFILLMENT_DATABASE", defaultDatabase))
	service := fulfillmentsvc.New(repository)
	if err := service.EnsureReady(ctx); err != nil {
		log.Fatalf("ensure indexes: %v", err)
	}

	consumer := kafkaconsumer.New(kafkaconsumer.Config{
		Brokers: env.CSV("KAFKA_BROKERS", defaultBrokers),
		Topic:   env.String("KAFKA_TOPIC", defaultTopic),
		GroupID: env.String("KAFKA_GROUP_ID", defaultGroupID),
	}, service)
	defer consumer.Close()

	log.Printf("fulfillment service waiting for topic %s", env.String("KAFKA_TOPIC", defaultTopic))
	if err := consumer.Run(ctx); err != nil {
		log.Fatalf("run consumer: %v", err)
	}
}
