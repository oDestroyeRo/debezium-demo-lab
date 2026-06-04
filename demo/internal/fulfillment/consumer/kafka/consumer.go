package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/odestroyero/debezium-demo-lab/internal/fulfillment/domain"
	fulfillmentsvc "github.com/odestroyero/debezium-demo-lab/internal/fulfillment/service"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

type Consumer struct {
	reader  *kafka.Reader
	service *fulfillmentsvc.Service
	groupID string
}

func New(config Config, service *fulfillmentsvc.Service) *Consumer {
	readerConfig := kafka.ReaderConfig{
		Brokers:        config.Brokers,
		Topic:          config.Topic,
		GroupID:        strings.TrimSpace(config.GroupID),
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	}

	return &Consumer{
		reader:  kafka.NewReader(readerConfig),
		service: service,
		groupID: readerConfig.GroupID,
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("fetch message: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if err := c.handleMessage(ctx, msg); err != nil {
			log.Printf("handle message topic=%s partition=%d offset=%d: %v", msg.Topic, msg.Partition, msg.Offset, err)
			continue
		}

		if c.groupID != "" {
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit message: %v", err)
			}
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	event, err := DecodeOrderCompleted(msg.Value, msg.Headers, msg.Key)
	if err != nil {
		return err
	}

	result, err := c.service.HandleOrderCompleted(ctx, event, domain.MessageMetadata{
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		Offset:     msg.Offset,
		Key:        string(msg.Key),
		Headers:    headersToMap(msg.Headers),
		ReceivedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}

	if result.Duplicate {
		log.Printf("duplicate event ignored id=%s order_id=%s offset=%d", result.EventID, result.OrderID, msg.Offset)
		return nil
	}

	log.Printf(
		"fulfillment started event_id=%s order_id=%s topic=%s partition=%d offset=%d",
		result.EventID,
		result.OrderID,
		msg.Topic,
		msg.Partition,
		msg.Offset,
	)
	return nil
}

func DecodeOrderCompleted(value []byte, headers []kafka.Header, key []byte) (domain.OrderCompletedEvent, error) {
	event, err := decodeEvent(value)
	if err != nil {
		return domain.OrderCompletedEvent{}, err
	}

	if event.EventID == "" {
		event.EventID = headerValue(headers, "id")
	}
	if event.EventID == "" {
		event.EventID = string(key)
	}
	if event.EventType == "" {
		event.EventType = headerValue(headers, "event_type")
	}
	if event.EventType == "" {
		event.EventType = domain.OrderCompletedEventType
	}
	if event.OrderID == "" {
		event.OrderID = event.AggregateID
	}
	return event, event.Validate()
}

func decodeEvent(value []byte) (domain.OrderCompletedEvent, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(value, &raw); err != nil {
		return domain.OrderCompletedEvent{}, fmt.Errorf("decode event json: %w", err)
	}

	if payload, ok := raw["payload"]; ok && raw["event_id"] == nil {
		var payloadString string
		if err := json.Unmarshal(payload, &payloadString); err == nil {
			return decodeEvent([]byte(payloadString))
		}
		return decodeEvent(payload)
	}

	var event domain.OrderCompletedEvent
	if err := bson.UnmarshalExtJSON(value, false, &event); err != nil {
		if jsonErr := json.Unmarshal(value, &event); jsonErr != nil {
			return domain.OrderCompletedEvent{}, fmt.Errorf("decode order completed event: %w", err)
		}
	}
	return event, nil
}

func headersToMap(headers []kafka.Header) map[string]string {
	out := make(map[string]string, len(headers))
	for _, header := range headers {
		out[header.Key] = string(header.Value)
	}
	return out
}

func headerValue(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value)
		}
	}
	return ""
}
