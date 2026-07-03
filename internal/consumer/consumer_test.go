package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/models"
)

type mockReader struct {
	index    int
	messages []kafka.Message
}

func newMockReader() *mockReader {
	return &mockReader{messages: []kafka.Message{}}
}

func (m *mockReader) AddMessage(msg kafka.Message) {
	m.messages = append(m.messages, msg)
}

func (m *mockReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if m.index >= len(m.messages) {
		return kafka.Message{}, context.Canceled
	}
	msg := m.messages[m.index]
	m.index++
	return msg, nil
}

func (m *mockReader) Close() error {
	return nil
}

type mockStorage struct {
	mu            sync.Mutex
	addOrderCalls []*models.Order
	addOrderErr   error
}

func newMockStorage() *mockStorage {
	return &mockStorage{}
}

func (m *mockStorage) AddOrderIfNotExists(ctx context.Context, order *models.Order) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addOrderCalls = append(m.addOrderCalls, order)
	return m.addOrderErr
}

func (m *mockStorage) AddOrder(ctx context.Context, order *models.Order) error {
	return nil
}

func (m *mockStorage) CheckHealthDB(ctx context.Context) error {
	return nil
}

func (m *mockStorage) GetOrderByUID(ctx context.Context, uid string) (*models.Order, error) {
	return nil, nil
}

func (m *mockStorage) GetAllOrdersUID(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (m *mockStorage) GetRecentOrdersUID(ctx context.Context, limit int) ([]string, error) {
	return nil, nil
}

func (m *mockStorage) OrderExists(ctx context.Context, uid string) (bool, error) {
	return false, nil
}

func validOrderJSON() []byte {
	order := models.Order{
		OrderUID:        "e1a2b3c4-d5e6-4f8a-9b0c-1d2e3f4a5b6c",
		TrackNumber:     "TRACK123",
		Entry:           "TEST",
		Locale:          "en",
		CustomerID:      "cust1",
		DeliveryService: "dhl",
		ShardKey:        "1",
		SMID:            1,
		DateCreated:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		OOFShard:        "1",
		Delivery: models.Delivery{
			Name:    "John",
			Phone:   "+79991234567",
			Zip:     "12345",
			City:    "City",
			Address: "Addr",
			Region:  "Reg",
			Email:   "test@example.com",
		},
		Payment: models.Payment{
			Transaction:  "b1e8a5d2-3c4f-4a1b-9d6e-7f8c0a1b2c3d",
			Currency:     "USD",
			Provider:     "stripe",
			Amount:       1000,
			PaymentDt:    1700000000,
			Bank:         "bank",
			DeliveryCost: 200,
			GoodsTotal:   800,
			CustomFee:    10,
		},
		Items: []models.Item{
			{ChrtID: 1, TrackNumber: "T1", Price: 100, Rid: "r1", Name: "Item", Sale: 0, Size: "M", TotalPrice: 100, NmID: 1, Brand: "B", Status: 200},
		},
	}
	data, _ := json.Marshal(order)
	return data
}

// TestProcessMessage_Valid checks that valid JSON is parsed into an Order.
func TestProcessMessage_Valid(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	c := NewConsumer(nil, nil, nil, logger)
	msg := kafka.Message{Value: validOrderJSON()}

	order, err := c.processMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if order.OrderUID != "e1a2b3c4-d5e6-4f8a-9b0c-1d2e3f4a5b6c" {
		t.Errorf("OrderUID mismatch: got %s", order.OrderUID)
	}
	if order.TrackNumber != "TRACK123" {
		t.Errorf("TrackNumber mismatch: got %s", order.TrackNumber)
	}
}

// TestProcessMessage_InvalidJSON checks that broken JSON returns an error.
func TestProcessMessage_InvalidJSON(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	c := NewConsumer(nil, nil, nil, logger)
	msg := kafka.Message{Value: []byte("not json")}

	_, err := c.processMessage(msg)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

// TestProcessMessage_ValidationFailed checks that an order missing required fields fails validation.
func TestProcessMessage_ValidationFailed(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	c := NewConsumer(nil, nil, nil, logger)
	msg := kafka.Message{Value: []byte(
		`{"order_uid":"x","track_number":"t","entry":"e","locale":"en",` +
			`"customer_id":"c","delivery_service":"d","shardkey":"1","sm_id":1,` +
			`"date_created":"2024-01-01T00:00:00Z","oof_shard":"1",` +
			`"delivery":{"name":"n","phone":"+79991234567","zip":"z",` +
			`"city":"c","address":"a","region":"r","email":"t@t.com"},` +
			`"payment":{"custom_fee":0},"items":[{"chrt_id":1,"track_number":"t",` +
			`"price":1,"rid":"r","name":"n","sale":0,"size":"s","total_price":1,` +
			`"nm_id":1,"brand":"b","status":200}]}`,
	)}

	_, err := c.processMessage(msg)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

// TestConsume_Successful checks that a valid message is stored and cached.
func TestConsume_Successful(t *testing.T) {
	reader := newMockReader()
	reader.AddMessage(kafka.Message{Value: validOrderJSON()})

	logger := slog.New(slog.DiscardHandler)
	storage := newMockStorage()
	cc := cache.NewCache(10, logger)
	c := NewConsumer(reader, storage, cc, logger)

	c.Consume(context.Background())

	if len(storage.addOrderCalls) != 1 {
		t.Fatalf("expected 1 AddOrder call, got %d", len(storage.addOrderCalls))
	}
	uid := storage.addOrderCalls[0].OrderUID
	if uid != "e1a2b3c4-d5e6-4f8a-9b0c-1d2e3f4a5b6c" {
		t.Errorf("unexpected OrderUID: %s", uid)
	}
	if _, exists := cc.Get(uid); !exists {
		t.Error("order should be in cache")
	}
}

// TestConsume_SkipInvalid checks that an invalid message is skipped and a valid one is processed.
func TestConsume_SkipInvalid(t *testing.T) {
	reader := newMockReader()
	reader.AddMessage(kafka.Message{Value: []byte("not json")})
	reader.AddMessage(kafka.Message{Value: validOrderJSON()})

	logger := slog.New(slog.DiscardHandler)
	storage := newMockStorage()
	c := NewConsumer(reader, storage, cache.NewCache(10, logger), logger)

	c.Consume(context.Background())

	if len(storage.addOrderCalls) != 1 {
		t.Fatalf("expected 1 AddOrder call, got %d", len(storage.addOrderCalls))
	}
}

// TestConsume_Duplicate checks that a duplicate order is not cached.
func TestConsume_Duplicate(t *testing.T) {
	reader := newMockReader()
	reader.AddMessage(kafka.Message{Value: validOrderJSON()})

	logger := slog.New(slog.DiscardHandler)
	storage := newMockStorage()
	storage.addOrderErr = errors.New("already exists")
	cc := cache.NewCache(10, logger)
	c := NewConsumer(reader, storage, cc, logger)

	c.Consume(context.Background())

	if _, exists := cc.Get("e1a2b3c4-d5e6-4f8a-9b0c-1d2e3f4a5b6c"); exists {
		t.Error("duplicate order should not be cached")
	}
}

// TestConsume_GracefulShutdown checks that the loop ends when the mock returns Canceled.
func TestConsume_GracefulShutdown(t *testing.T) {
	reader := newMockReader()
	reader.AddMessage(kafka.Message{Value: validOrderJSON()})
	reader.AddMessage(kafka.Message{Value: validOrderJSON()})
	reader.AddMessage(kafka.Message{Value: validOrderJSON()})

	logger := slog.New(slog.DiscardHandler)
	storage := newMockStorage()
	c := NewConsumer(reader, storage, cache.NewCache(10, logger), logger)

	c.Consume(context.Background())

	if len(storage.addOrderCalls) != 3 {
		t.Errorf("expected 3 AddOrder calls, got %d", len(storage.addOrderCalls))
	}
}