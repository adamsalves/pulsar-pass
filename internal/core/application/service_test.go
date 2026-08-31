package application_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/core/application"
	"github.com/adamsalves/pulsar-pass/internal/core/domain"
	"github.com/adamsalves/pulsar-pass/pkg/envelope"
)

type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time { return f.now }

type fakeReservations struct {
	store map[string]*domain.Reservation
}

func newFakeReservations() *fakeReservations {
	return &fakeReservations{store: make(map[string]*domain.Reservation)}
}

func (f *fakeReservations) Create(_ context.Context, r *domain.Reservation) error {
	f.store[r.ID] = r
	return nil
}

func (f *fakeReservations) Get(_ context.Context, id string) (*domain.Reservation, error) {
	r, ok := f.store[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *r
	return &cp, nil
}

func (f *fakeReservations) Update(_ context.Context, r *domain.Reservation) error {
	if _, ok := f.store[r.ID]; !ok {
		return domain.ErrNotFound
	}
	f.store[r.ID] = r
	return nil
}

type fakeInventory struct{ events map[string]*domain.Event }

func newFakeInventory(events ...*domain.Event) *fakeInventory {
	m := make(map[string]*domain.Event, len(events))
	for _, e := range events {
		m[e.ID] = e
	}
	return &fakeInventory{events: m}
}

func (f *fakeInventory) Event(_ context.Context, id string) (*domain.Event, error) {
	e, ok := f.events[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *e
	return &cp, nil
}

func (f *fakeInventory) ReserveCapacity(_ context.Context, eventID string, n int) error {
	e, ok := f.events[eventID]
	if !ok {
		return domain.ErrNotFound
	}
	return e.Reserve(n)
}

func (f *fakeInventory) ReleaseCapacity(_ context.Context, eventID string, n int) error {
	e, ok := f.events[eventID]
	if !ok {
		return domain.ErrNotFound
	}
	return e.Release(n)
}

func (f *fakeInventory) CommitSold(_ context.Context, eventID string, n int) error {
	e, ok := f.events[eventID]
	if !ok {
		return domain.ErrNotFound
	}
	return e.ConfirmSold(n)
}

type fakeOutbox struct{ records []application.OutboxRecord }

func (f *fakeOutbox) Enqueue(_ context.Context, records ...application.OutboxRecord) error {
	f.records = append(f.records, records...)
	return nil
}

func testEvent(id string, capacity int) *domain.Event {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	return &domain.Event{
		ID:          id,
		Name:        "Show Teste",
		Venue:       "Arena",
		StartsAt:    base.Add(24 * time.Hour),
		SaleOpensAt: base.Add(-time.Hour),
		PriceCents:  1000,
		Capacity:    capacity,
	}
}

func newService(clock *fakeClock, inventory *fakeInventory, outbox *fakeOutbox) *application.ReservationService {
	return application.NewReservationService(
		newFakeReservations(),
		inventory,
		outbox,
		nil,
		clock,
		10*time.Minute,
	)
}

func TestReserveCreatesPendingReservationWithOutbox(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	outbox := &fakeOutbox{}
	inv := newFakeInventory(testEvent("event-1", 10))
	svc := newService(clock, inv, outbox)

	res, err := svc.Reserve(context.Background(), application.ReserveCommand{
		EventID:  "event-1",
		UserID:   "user-1",
		Quantity: 2,
	})
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if res.Status != domain.ReservationStatusPending {
		t.Errorf("status = %q, want PENDING", res.Status)
	}
	if res.AmountCents != 2000 {
		t.Errorf("amount = %d, want 2000 (price * quantity)", res.AmountCents)
	}
	if !res.ExpiresAt.After(clock.now) {
		t.Errorf("expires_at %v should be in the future", res.ExpiresAt)
	}
	if inv.events["event-1"].ReservedCount != 2 {
		t.Errorf("reserved_count = %d, want 2", inv.events["event-1"].ReservedCount)
	}
	if len(outbox.records) != 1 {
		t.Fatalf("outbox records = %d, want 1", len(outbox.records))
	}
	rec := outbox.records[0]
	if rec.EventType != envelope.TypeTicketReserved {
		t.Errorf("event_type = %q, want %q", rec.EventType, envelope.TypeTicketReserved)
	}
	if rec.Subject != envelope.SubjectTicketReserved {
		t.Errorf("subject = %q, want %q", rec.Subject, envelope.SubjectTicketReserved)
	}
	if rec.CorrelationID != res.ID {
		t.Errorf("correlation_id = %q, want reservation id %q", rec.CorrelationID, res.ID)
	}
	if !strings.Contains(string(rec.Payload), `"currency":"BRL"`) {
		t.Errorf("payload missing currency: %s", rec.Payload)
	}
}

func TestReserveUsesClientProvidedID(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	inv := newFakeInventory(testEvent("event-1", 10))
	svc := newService(clock, inv, &fakeOutbox{})

	res, err := svc.Reserve(context.Background(), application.ReserveCommand{
		ReservationID: "client-id-1",
		EventID:       "event-1",
		UserID:        "user-1",
		Quantity:      1,
	})
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if res.ID != "client-id-1" {
		t.Errorf("id = %q, want client-provided id", res.ID)
	}
}

func TestReserveRejectsWhenSoldOut(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	outbox := &fakeOutbox{}
	inv := newFakeInventory(testEvent("event-1", 1))
	svc := newService(clock, inv, outbox)

	cmd := application.ReserveCommand{EventID: "event-1", UserID: "user-1", Quantity: 1}
	if _, err := svc.Reserve(context.Background(), cmd); err != nil {
		t.Fatalf("first Reserve() error = %v", err)
	}
	if _, err := svc.Reserve(context.Background(), cmd); !errors.Is(err, domain.ErrSoldOut) {
		t.Errorf("second Reserve() error = %v, want ErrSoldOut", err)
	}
	if len(outbox.records) != 1 {
		t.Errorf("outbox records = %d, want 1 (rejected reserve must not enqueue)", len(outbox.records))
	}
}

func TestReserveRejectsClosedSale(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	event := testEvent("event-1", 10)
	event.SaleOpensAt = clock.now.Add(time.Hour)
	svc := newService(clock, newFakeInventory(event), &fakeOutbox{})

	_, err := svc.Reserve(context.Background(), application.ReserveCommand{
		EventID: "event-1", UserID: "user-1", Quantity: 1,
	})
	if !errors.Is(err, domain.ErrSaleNotOpen) {
		t.Errorf("error = %v, want ErrSaleNotOpen", err)
	}
}

func TestConfirmFlow(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	outbox := &fakeOutbox{}
	inv := newFakeInventory(testEvent("event-1", 10))
	svc := newService(clock, inv, outbox)

	res, err := svc.Reserve(context.Background(), application.ReserveCommand{
		EventID: "event-1", UserID: "user-1", Quantity: 2,
	})
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	confirmed, err := svc.Confirm(context.Background(), res.ID)
	if err != nil {
		t.Fatalf("Confirm() error = %v", err)
	}
	if confirmed.Status != domain.ReservationStatusConfirmed {
		t.Errorf("status = %q, want CONFIRMED", confirmed.Status)
	}
	if inv.events["event-1"].SoldCount != 2 {
		t.Errorf("sold_count = %d, want 2", inv.events["event-1"].SoldCount)
	}
	if inv.events["event-1"].ReservedCount != 0 {
		t.Errorf("reserved_count = %d, want 0", inv.events["event-1"].ReservedCount)
	}
	last := outbox.records[len(outbox.records)-1]
	if last.EventType != envelope.TypeTicketConfirmed {
		t.Errorf("event_type = %q, want %q", last.EventType, envelope.TypeTicketConfirmed)
	}
}

func TestConfirmAfterExpiryFails(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: base}
	inv := newFakeInventory(testEvent("event-1", 10))
	svc := newService(clock, inv, &fakeOutbox{})

	res, err := svc.Reserve(context.Background(), application.ReserveCommand{
		EventID: "event-1", UserID: "user-1", Quantity: 1,
	})
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	clock.now = base.Add(11 * time.Minute)
	if _, err := svc.Confirm(context.Background(), res.ID); !errors.Is(err, domain.ErrReservationExpired) {
		t.Errorf("error = %v, want ErrReservationExpired", err)
	}
}

func TestExpireFlowReleasesCapacity(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: base}
	outbox := &fakeOutbox{}
	inv := newFakeInventory(testEvent("event-1", 10))
	svc := newService(clock, inv, outbox)

	res, err := svc.Reserve(context.Background(), application.ReserveCommand{
		EventID: "event-1", UserID: "user-1", Quantity: 3,
	})
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	clock.now = base.Add(11 * time.Minute)
	expired, err := svc.Expire(context.Background(), res.ID)
	if err != nil {
		t.Fatalf("Expire() error = %v", err)
	}
	if expired.Status != domain.ReservationStatusExpired {
		t.Errorf("status = %q, want EXPIRED", expired.Status)
	}
	if inv.events["event-1"].ReservedCount != 0 {
		t.Errorf("reserved_count = %d, want 0", inv.events["event-1"].ReservedCount)
	}
	last := outbox.records[len(outbox.records)-1]
	if last.EventType != envelope.TypeTicketReleased {
		t.Errorf("event_type = %q, want %q", last.EventType, envelope.TypeTicketReleased)
	}
	if !strings.Contains(string(last.Payload), `"reason":"expired"`) {
		t.Errorf("payload missing reason expired: %s", last.Payload)
	}
}

func TestFailAndCancelFlows(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: base}
	inv := newFakeInventory(testEvent("event-1", 10))
	outbox := &fakeOutbox{}
	svc := newService(clock, inv, outbox)

	first, err := svc.Reserve(context.Background(), application.ReserveCommand{
		EventID: "event-1", UserID: "user-1", Quantity: 1,
	})
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	failed, err := svc.Fail(context.Background(), first.ID)
	if err != nil {
		t.Fatalf("Fail() error = %v", err)
	}
	if failed.Status != domain.ReservationStatusFailed {
		t.Errorf("status = %q, want FAILED", failed.Status)
	}
	if inv.events["event-1"].ReservedCount != 0 {
		t.Errorf("reserved_count = %d, want 0 after fail", inv.events["event-1"].ReservedCount)
	}

	second, err := svc.Reserve(context.Background(), application.ReserveCommand{
		EventID: "event-1", UserID: "user-2", Quantity: 1,
	})
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	cancelled, err := svc.Cancel(context.Background(), second.ID)
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if cancelled.Status != domain.ReservationStatusCancelled {
		t.Errorf("status = %q, want CANCELLED", cancelled.Status)
	}
}

func TestDoubleConfirmFails(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	inv := newFakeInventory(testEvent("event-1", 10))
	svc := newService(clock, inv, &fakeOutbox{})

	res, err := svc.Reserve(context.Background(), application.ReserveCommand{
		EventID: "event-1", UserID: "user-1", Quantity: 1,
	})
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if _, err := svc.Confirm(context.Background(), res.ID); err != nil {
		t.Fatalf("first Confirm() error = %v", err)
	}
	if _, err := svc.Confirm(context.Background(), res.ID); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("second Confirm() error = %v, want ErrInvalidTransition", err)
	}
}
