package mongorepository

import (
	"context"
	"errors"

	"github.com/odestroyero/debezium-demo-lab/internal/fulfillment/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

var errDuplicateEvent = errors.New("duplicate event")

type Repository struct {
	client          *mongo.Client
	processedEvents *mongo.Collection
	fulfillments    *mongo.Collection
}

func New(client *mongo.Client, database string) *Repository {
	db := client.Database(database)
	return &Repository{
		client:          client,
		processedEvents: db.Collection("processed_events"),
		fulfillments:    db.Collection("fulfillments"),
	}
}

func (r *Repository) EnsureIndexes(ctx context.Context) error {
	if _, err := r.processedEvents.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "event_type", Value: 1}, {Key: "received_at", Value: -1}},
	}); err != nil {
		return err
	}

	_, err := r.fulfillments.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "order_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}

func (r *Repository) RecordOrderCompleted(ctx context.Context, event domain.OrderCompletedEvent, metadata domain.MessageMetadata) (bool, error) {
	session, err := r.client.StartSession()
	if err != nil {
		return false, err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(
		ctx,
		func(sc context.Context) (any, error) {
			if _, err := r.processedEvents.InsertOne(sc, processedEventDocument(event, metadata)); err != nil {
				if mongo.IsDuplicateKeyError(err) {
					return nil, errDuplicateEvent
				}
				return nil, err
			}

			_, err := r.fulfillments.UpdateOne(
				sc,
				bson.M{"order_id": event.OrderID},
				bson.M{
					"$setOnInsert": bson.M{
						"_id":          "ful_" + event.OrderID,
						"order_id":     event.OrderID,
						"checkout_id":  event.CheckoutID,
						"shop_id":      event.ShopID,
						"total_amount": event.TotalAmount,
						"status":       "STARTED",
						"created_at":   metadata.ReceivedAt,
					},
					"$set": bson.M{
						"last_event_id": event.EventID,
						"trace_id":      event.TraceID,
						"updated_at":    metadata.ReceivedAt,
					},
				},
				options.UpdateOne().SetUpsert(true),
			)
			return nil, err
		},
		options.Transaction().SetWriteConcern(writeconcern.Majority()),
	)
	if errors.Is(err, errDuplicateEvent) {
		return true, nil
	}
	return false, err
}

func processedEventDocument(event domain.OrderCompletedEvent, metadata domain.MessageMetadata) bson.M {
	return bson.M{
		"_id":            event.EventID,
		"event_type":     event.EventType,
		"event_version":  event.EventVersion,
		"aggregate_type": event.AggregateType,
		"aggregate_id":   event.AggregateID,
		"order_id":       event.OrderID,
		"checkout_id":    event.CheckoutID,
		"topic":          metadata.Topic,
		"partition":      metadata.Partition,
		"offset":         metadata.Offset,
		"message_key":    metadata.Key,
		"headers":        metadata.Headers,
		"occurred_at":    event.OccurredAt,
		"received_at":    metadata.ReceivedAt,
	}
}
