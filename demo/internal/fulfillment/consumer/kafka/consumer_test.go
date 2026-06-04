package kafka

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestDecodeOrderCompletedFromPayloadEnvelope(t *testing.T) {
	value := []byte(`{
		"payload": {
			"event_id": "evt_1",
			"event_type": "order.completed",
			"aggregate_id": "ord_1",
			"checkout_id": "checkout-1001",
			"order_id": "ord_1",
			"shop_id": "shop_77",
			"total_amount": 2490
		}
	}`)

	event, err := DecodeOrderCompleted(value, nil, nil)
	if err != nil {
		t.Fatalf("DecodeOrderCompleted() error = %v", err)
	}
	if event.EventID != "evt_1" || event.OrderID != "ord_1" {
		t.Fatalf("unexpected event: %+v", event)
	}
}

func TestDecodeOrderCompletedUsesHeaderFallbacks(t *testing.T) {
	value := []byte(`{
		"checkout_id": "checkout-1001",
		"order_id": "ord_1",
		"shop_id": "shop_77",
		"total_amount": 2490
	}`)
	headers := []kafka.Header{
		{Key: "id", Value: []byte("evt_header")},
		{Key: "event_type", Value: []byte("order.completed")},
	}

	event, err := DecodeOrderCompleted(value, headers, []byte("ord_1"))
	if err != nil {
		t.Fatalf("DecodeOrderCompleted() error = %v", err)
	}
	if event.EventID != "evt_header" || event.EventType != "order.completed" {
		t.Fatalf("unexpected event fallback fields: %+v", event)
	}
}
