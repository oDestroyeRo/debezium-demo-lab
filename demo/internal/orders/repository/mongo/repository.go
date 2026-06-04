package mongorepository

import (
	"context"

	"github.com/odestroyero/debezium-demo-lab/internal/orders/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type Repository struct {
	client *mongo.Client
	orders *mongo.Collection
	outbox *mongo.Collection
}

func New(client *mongo.Client, database string) *Repository {
	db := client.Database(database)
	return &Repository{
		client: client,
		orders: db.Collection("orders"),
		outbox: db.Collection("outbox_events"),
	}
}

func (r *Repository) EnsureIndexes(ctx context.Context) error {
	if _, err := r.orders.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "checkout_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return err
	}

	_, err := r.outbox.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "aggregateid", Value: 1}}},
		{Keys: bson.D{{Key: "aggregatetype", Value: 1}}},
	})
	return err
}

func (r *Repository) CompleteCheckout(ctx context.Context, order domain.Order, event domain.OutboxEvent) error {
	session, err := r.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(
		ctx,
		func(sc context.Context) (any, error) {
			if _, err := r.orders.InsertOne(sc, orderDocument(order)); err != nil {
				return nil, err
			}
			_, err := r.outbox.InsertOne(sc, outboxDocument(event))
			return nil, err
		},
		options.Transaction().SetWriteConcern(writeconcern.Majority()),
	)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return domain.ErrCheckoutAlreadyCompleted
		}
		return err
	}
	return nil
}

func orderDocument(order domain.Order) bson.M {
	return bson.M{
		"_id":          order.ID,
		"checkout_id":  order.CheckoutID,
		"shop_id":      order.ShopID,
		"status":       order.Status,
		"total_amount": order.TotalAmount,
		"created_at":   order.CreatedAt,
		"completed_at": order.CompletedAt,
	}
}

func outboxDocument(event domain.OutboxEvent) bson.M {
	return bson.M{
		"_id":           event.ID,
		"aggregatetype": event.AggregateType,
		"aggregateid":   event.AggregateID,
		"type":          event.Type,
		"occurred_at":   event.OccurredAt,
		"payload":       event.Payload,
	}
}
