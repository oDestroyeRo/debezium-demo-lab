package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/odestroyero/debezium-demo-lab/internal/orders/domain"
)

type fakeOrdersRepository struct {
	order domain.Order
	event domain.OutboxEvent
	err   error
}

func (r *fakeOrdersRepository) EnsureIndexes(context.Context) error {
	return nil
}

func (r *fakeOrdersRepository) CompleteCheckout(_ context.Context, order domain.Order, event domain.OutboxEvent) error {
	r.order = order
	r.event = event
	return r.err
}

func TestCompleteCheckoutWritesOrderAndOutboxIntent(t *testing.T) {
	repo := &fakeOrdersRepository{}
	now := time.Date(2026, 6, 4, 10, 30, 0, 0, time.UTC)
	ids := []string{"ord_1", "evt_1", "trace_1"}
	service := New(
		repo,
		WithClock(func() time.Time { return now }),
		WithIDGenerator(func(string) string {
			id := ids[0]
			ids = ids[1:]
			return id
		}),
	)

	result, err := service.CompleteCheckout(context.Background(), domain.CompleteCheckoutCommand{
		CheckoutID:  "checkout-1001",
		ShopID:      "shop_77",
		TotalAmount: 2490,
	})
	if err != nil {
		t.Fatalf("CompleteCheckout() error = %v", err)
	}

	if result.OrderID != "ord_1" || result.OutboxEventID != "evt_1" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if repo.order.ID != "ord_1" || repo.order.Status != "COMPLETED" {
		t.Fatalf("unexpected order document: %+v", repo.order)
	}
	if repo.event.AggregateID != "ord_1" || repo.event.Payload.EventID != "evt_1" {
		t.Fatalf("unexpected outbox event: %+v", repo.event)
	}
	if repo.event.Payload.TraceID != "trace_1" {
		t.Fatalf("unexpected trace id: %s", repo.event.Payload.TraceID)
	}
}

func TestCompleteCheckoutValidatesCommand(t *testing.T) {
	service := New(&fakeOrdersRepository{})

	_, err := service.CompleteCheckout(context.Background(), domain.CompleteCheckoutCommand{})
	if !errors.Is(err, domain.ErrInvalidCheckoutCommand) {
		t.Fatalf("expected invalid command, got %v", err)
	}
}
