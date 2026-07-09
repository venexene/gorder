package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/models"
)

func setTestUser(c *gin.Context, userID string) {
	c.Set("user_id", userID)
}

type mockStorage struct {
	healthError error
}

func (m *mockStorage) CheckHealthDB(ctx context.Context) error {
	return m.healthError
}

func (m *mockStorage) GetOrderByUID(ctx context.Context, orderUID string) (*models.Order, error) {
	if orderUID == "exist" {
		return &models.Order{OrderUID: "exist", TrackNumber: "TRACK"}, nil
	}
	return nil, pgx.ErrNoRows
}

func (m *mockStorage) GetAllOrdersUID(ctx context.Context) ([]string, error) {
	return []string{"order1", "order2"}, nil
}

func (m *mockStorage) GetRecentOrdersUID(ctx context.Context, limit int) ([]string, error) {
	return []string{"order1", "order2"}, nil
}

func (m *mockStorage) OrderExists(ctx context.Context, orderUID string) (bool, error) {
	return orderUID == "exist", nil
}

func (m *mockStorage) AddOrder(ctx context.Context, order *models.Order) error {
	return nil
}

func (m *mockStorage) AddOrderIfNotExists(ctx context.Context, order *models.Order) error {
	return nil
}

func (m *mockStorage) CreateUser(ctx context.Context, user *models.User) error {
	return nil
}

func (m *mockStorage) GetUser(ctx context.Context, username string) (*models.User, error) {
	return nil, nil
}

type mockConsumer struct {
	healthError error
}

func (m *mockConsumer) CheckHealth(ctx context.Context) error {
	return m.healthError
}

func newTestHandler() *Handler {
	logger := slog.New(slog.DiscardHandler)
	hd := &HandlerDependencies{
		Storage:  &mockStorage{},
		Consumer: &mockConsumer{},
		Cache:    cache.NewCache(10, logger, nil),
		Logger:   logger,
		Config:   nil,
	}
	return NewHandler(hd)
}

// TestLiveCheckHandle verifies 200 returns by liveness check.
func TestLiveCheckHandle(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/health", nil)

	handler.LiveCheckHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// TestReadyCheckHandle_Up verifies 200 UP when both DB and Kafka are healthy.
func TestReadyCheckHandle_Up(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/health", nil)

	handler.ReadyCheckHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["status"] != "UP" {
		t.Errorf("expected status UP, got %s", body["status"])
	}
}

// TestReadyCheckHandle_KafkaDown verifies 503 DOWN when Kafka is unhealthy.
func TestReadyCheckHandle_KafkaDown(t *testing.T) {
	handler := newTestHandler()
	handler.consumer = &mockConsumer{healthError: fmt.Errorf("kafka down")}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/health", nil)

	handler.ReadyCheckHandle(c)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}

	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["status"] != "DOWN" {
		t.Errorf("expected status DOWN, got %s", body["status"])
	}
}

// TestReadyCheckHandle_DBDown verifies 503 DOWN when database is unhealthy.
func TestReadyCheckHandle_DBDown(t *testing.T) {
	handler := newTestHandler()
	handler.storage = &mockStorage{healthError: fmt.Errorf("db down")}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/health", nil)

	handler.ReadyCheckHandle(c)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}

	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["status"] != "DOWN" {
		t.Errorf("expected status DOWN, got %s", body["status"])
	}
}

// TestGetOrderByUIDHandle_NoAuth verifies 401 when user_id is missing from context.
func TestGetOrderByUIDHandle_NoAuth(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Params = []gin.Param{{Key: "uid", Value: "exist"}}

	handler.GetOrderByUIDHandle(c)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

// TestGetAllOrdersUIDHandle_NoAuth verifies 401 when user_id is missing.
func TestGetAllOrdersUIDHandle_NoAuth(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)

	handler.GetAllOrdersUIDHandle(c)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

// TestGetOrderByUIDHandle_Existing checks known UID.
func TestGetOrderByUIDHandle_Existing(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Params = []gin.Param{{Key: "uid", Value: "exist"}}
	setTestUser(c, "test-user")

	handler.GetOrderByUIDHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var order models.Order
	json.NewDecoder(w.Body).Decode(&order)
	if order.OrderUID != "exist" {
		t.Errorf("expected OrderUID 'exist', got %s", order.OrderUID)
	}
	if order.TrackNumber != "TRACK" {
		t.Errorf("expected TrackNumber 'TRACK', got %s", order.TrackNumber)
	}
}

// TestGetOrderByUIDHandle_Missing checks unknown UID.
func TestGetOrderByUIDHandle_Missing(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Params = []gin.Param{{Key: "uid", Value: "nosuchorder"}}
	setTestUser(c, "test-user")

	handler.GetOrderByUIDHandle(c)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

// TestGetOrderByUIDHandle_EmptyUID checks empty UID.
func TestGetOrderByUIDHandle_EmptyUID(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Params = []gin.Param{{Key: "uid", Value: ""}}
	setTestUser(c, "test-user")

	handler.GetOrderByUIDHandle(c)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// TestGetOrderByUIDHandle_CacheHit checks that a cached order  works without db query.
func TestGetOrderByUIDHandle_CacheHit(t *testing.T) {
	handler := newTestHandler()
	testOrder := &models.Order{OrderUID: "cached-uid", TrackNumber: "CACHED"}
	handler.cache.Set(testOrder)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Params = []gin.Param{{Key: "uid", Value: "cached-uid"}}
	setTestUser(c, "test-user")

	handler.GetOrderByUIDHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var order models.Order
	json.NewDecoder(w.Body).Decode(&order)
	if order.TrackNumber != "CACHED" {
		t.Errorf("expected TrackNumber 'CACHED', got %s", order.TrackNumber)
	}
}

// TestGetAllOrdersUIDHandle checks UID list endpoint.
func TestGetAllOrdersUIDHandle(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	setTestUser(c, "test-user")

	handler.GetAllOrdersUIDHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var body map[string][]string
	json.NewDecoder(w.Body).Decode(&body)
	if len(body["order_uids"]) != 2 {
		t.Errorf("expected 2 UIDs, got %d", len(body["order_uids"]))
	}
}
