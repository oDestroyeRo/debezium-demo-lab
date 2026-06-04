package domain

import (
	"errors"
	"strings"
	"time"
)

const (
	OrderCompletedEventType = "order.completed"
	OrderAggregateType      = "order"
	OrdersRoute             = "orders"
	OrdersTopic             = "orders.events.v1"
)

var (
	ErrCheckoutAlreadyCompleted = errors.New("checkout already completed")
	ErrInvalidCheckoutCommand   = errors.New("invalid checkout command")
)

type CompleteCheckoutCommand struct {
	CheckoutID  string
	ShopID      string
	TotalAmount int64
}

func (cmd CompleteCheckoutCommand) Validate() error {
	if strings.TrimSpace(cmd.CheckoutID) == "" {
		return ErrInvalidCheckoutCommand
	}
	if strings.TrimSpace(cmd.ShopID) == "" {
		return ErrInvalidCheckoutCommand
	}
	if cmd.TotalAmount <= 0 {
		return ErrInvalidCheckoutCommand
	}
	return nil
}

type Order struct {
	ID          string
	CheckoutID  string
	ShopID      string
	Status      string
	TotalAmount int64
	CreatedAt   time.Time
	CompletedAt time.Time
}

type OutboxEvent struct {
	ID            string
	AggregateType string
	AggregateID   string
	Type          string
	OccurredAt    time.Time
	Payload       OrderCompletedPayload
}

type OrderCompletedPayload struct {
	EventID       string    `json:"event_id" bson:"event_id"`
	EventType     string    `json:"event_type" bson:"event_type"`
	AggregateType string    `json:"aggregate_type" bson:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id" bson:"aggregate_id"`
	EventVersion  int       `json:"event_version" bson:"event_version"`
	CheckoutID    string    `json:"checkout_id" bson:"checkout_id"`
	OrderID       string    `json:"order_id" bson:"order_id"`
	ShopID        string    `json:"shop_id" bson:"shop_id"`
	TotalAmount   int64     `json:"total_amount" bson:"total_amount"`
	TraceID       string    `json:"trace_id" bson:"trace_id"`
	OccurredAt    time.Time `json:"occurred_at" bson:"occurred_at"`
}

type CompleteCheckoutResult struct {
	CheckoutID    string
	OrderID       string
	OutboxEventID string
	Topic         string
}
