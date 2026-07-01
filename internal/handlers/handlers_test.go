package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/models"
)

type mockStorage struct{}

func (m *mockStorage) TestDB(ctx context.Context) (string, error) {
	return "Database works", nil
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

func newTestHandler() *Handler {
	return NewHandler(&mockStorage{}, cache.NewCache(10), "")
}

// TestServerHandle checks the server health-check endpoint.
func TestServerHandle(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)

	handler.TestServerHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["status"] != "Server definetly works" {
		t.Errorf("unexpected status: %s", body["status"])
	}
}

// TestDBHandle checks the database health-check endpoint.
func TestDBHandle(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)

	handler.TestDBHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["status"] != "Database works" {
		t.Errorf("unexpected status: %s", body["status"])
	}
}

// TestKafkaHandle_Failure checks unreachable broker.
func TestKafkaHandle_Failure(t *testing.T) {
	handler := NewHandler(&mockStorage{}, cache.NewCache(10), "localhost:19999")
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)

	handler.TestKafkaHandle(c)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

// TestGetOrderByUIDHandle_Existing checks known UID.
func TestGetOrderByUIDHandle_Existing(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Params = []gin.Param{{Key: "uid", Value: "exist"}}

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
