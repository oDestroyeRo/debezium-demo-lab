package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/odestroyero/debezium-demo-lab/internal/orders/domain"
)

type Repository interface {
	EnsureIndexes(ctx context.Context) error
	CompleteCheckout(ctx context.Context, order domain.Order, event domain.OutboxEvent) error
}

type Service struct {
	repo  Repository
	now   func() time.Time
	newID func(prefix string) string
}

type Option func(*Service)

func New(repo Repository, opts ...Option) *Service {
	s := &Service{
		repo:  repo,
		now:   func() time.Time { return time.Now().UTC() },
		newID: randomID,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func WithClock(now func() time.Time) Option {
	return func(s *Service) {
		s.now = now
	}
}

func WithIDGenerator(newID func(prefix string) string) Option {
	return func(s *Service) {
		s.newID = newID
	}
}

func (s *Service) EnsureReady(ctx context.Context) error {
	return s.repo.EnsureIndexes(ctx)
}

func (s *Service) CompleteCheckout(ctx context.Context, cmd domain.CompleteCheckoutCommand) (domain.CompleteCheckoutResult, error) {
	if err := cmd.Validate(); err != nil {
		return domain.CompleteCheckoutResult{}, err
	}

	now := s.now()
	orderID := s.newID("ord")
	eventID := s.newID("evt")
	traceID := s.newID("trace")

	order := domain.Order{
		ID:          orderID,
		CheckoutID:  cmd.CheckoutID,
		ShopID:      cmd.ShopID,
		Status:      "COMPLETED",
		TotalAmount: cmd.TotalAmount,
		CreatedAt:   now,
		CompletedAt: now,
	}
	event := domain.OutboxEvent{
		ID:            eventID,
		AggregateType: domain.OrdersRoute,
		AggregateID:   orderID,
		Type:          domain.OrderCompletedEventType,
		OccurredAt:    now,
		Payload: domain.OrderCompletedPayload{
			EventID:       eventID,
			EventType:     domain.OrderCompletedEventType,
			AggregateType: domain.OrderAggregateType,
			AggregateID:   orderID,
			EventVersion:  1,
			CheckoutID:    cmd.CheckoutID,
			OrderID:       orderID,
			ShopID:        cmd.ShopID,
			TotalAmount:   cmd.TotalAmount,
			TraceID:       traceID,
			OccurredAt:    now,
		},
	}

	if err := s.repo.CompleteCheckout(ctx, order, event); err != nil {
		return domain.CompleteCheckoutResult{}, err
	}

	return domain.CompleteCheckoutResult{
		CheckoutID:    cmd.CheckoutID,
		OrderID:       orderID,
		OutboxEventID: eventID,
		Topic:         domain.OrdersTopic,
	}, nil
}

func randomID(prefix string) string {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%s_local_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(buf[:])
}
