package kafka

import (
	"testing"
	"time"

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

func TestDecodeOrderCompletedFromDebeziumMongoPayloadString(t *testing.T) {
	value := []byte(`{
		"payload": "{\n  \"event_id\": \"evt_bd674e6edc7c63655154a16f\",\n  \"event_type\": \"order.completed\",\n  \"aggregate_type\": \"order\",\n  \"aggregate_id\": \"ord_3c4d8d698b03a074548c3e40\",\n  \"event_version\": {\n    \"$numberInt\": \"1\"\n  },\n  \"checkout_id\": \"checkout-demo-001\",\n  \"order_id\": \"ord_3c4d8d698b03a074548c3e40\",\n  \"shop_id\": \"shop_77\",\n  \"total_amount\": {\n    \"$numberLong\": \"2490\"\n  },\n  \"trace_id\": \"trace_4158470ad2b773175e56fee7\",\n  \"occurred_at\": {\n    \"$date\": {\n      \"$numberLong\": \"1780548771570\"\n    }\n  }\n}",
		"occurred_at": 1780548771570
	}`)

	event, err := DecodeOrderCompleted(value, nil, nil)
	if err != nil {
		t.Fatalf("DecodeOrderCompleted() error = %v", err)
	}
	if event.EventID != "evt_bd674e6edc7c63655154a16f" || event.OrderID != "ord_3c4d8d698b03a074548c3e40" {
		t.Fatalf("unexpected event identity: %+v", event)
	}
	if event.EventVersion != 1 || event.TotalAmount != 2490 {
		t.Fatalf("unexpected numeric fields: %+v", event)
	}
	if event.OccurredAt.IsZero() || event.OccurredAt.Location() != time.UTC {
		t.Fatalf("unexpected occurred_at: %v", event.OccurredAt)
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
