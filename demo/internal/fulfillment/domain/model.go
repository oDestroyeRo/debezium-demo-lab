package domain

import (
	"errors"
	"time"
)

const OrderCompletedEventType = "order.completed"

var (
	ErrInvalidOrderCompletedEvent = errors.New("invalid order completed event")
	ErrUnsupportedEventType       = errors.New("unsupported event type")
)

type OrderCompletedEvent struct {
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

func (e OrderCompletedEvent) Validate() error {
	if e.EventType != "" && e.EventType != OrderCompletedEventType {
		return ErrUnsupportedEventType
	}
	if e.EventID == "" || e.OrderID == "" || e.CheckoutID == "" {
		return ErrInvalidOrderCompletedEvent
	}
	if e.EventType == "" {
		e.EventType = OrderCompletedEventType
	}
	return nil
}

type MessageMetadata struct {
	Topic      string
	Partition  int
	Offset     int64
	Key        string
	Headers    map[string]string
	ReceivedAt time.Time
}

type HandleResult struct {
	Duplicate bool
	EventID   string
	OrderID   string
}
