package service

import (
	"context"
	"time"

	"github.com/odestroyero/debezium-demo-lab/internal/fulfillment/domain"
)

type Repository interface {
	EnsureIndexes(ctx context.Context) error
	RecordOrderCompleted(ctx context.Context, event domain.OrderCompletedEvent, metadata domain.MessageMetadata) (bool, error)
}

type Service struct {
	repo Repository
	now  func() time.Time
}

type Option func(*Service)

func New(repo Repository, opts ...Option) *Service {
	s := &Service{
		repo: repo,
		now:  func() time.Time { return time.Now().UTC() },
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

func (s *Service) EnsureReady(ctx context.Context) error {
	return s.repo.EnsureIndexes(ctx)
}

func (s *Service) HandleOrderCompleted(ctx context.Context, event domain.OrderCompletedEvent, metadata domain.MessageMetadata) (domain.HandleResult, error) {
	if event.EventType == "" {
		event.EventType = domain.OrderCompletedEventType
	}
	if metadata.ReceivedAt.IsZero() {
		metadata.ReceivedAt = s.now()
	}
	if err := event.Validate(); err != nil {
		return domain.HandleResult{}, err
	}

	duplicate, err := s.repo.RecordOrderCompleted(ctx, event, metadata)
	if err != nil {
		return domain.HandleResult{}, err
	}
	return domain.HandleResult{
		Duplicate: duplicate,
		EventID:   event.EventID,
		OrderID:   event.OrderID,
	}, nil
}
