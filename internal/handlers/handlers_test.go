package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/config"
	"github.com/venexene/gorder/internal/models"
)


// Мок для базы данных
type mockStorage struct{}

func (m *mockStorage) TestDB(ctx context.Context) (string, error) {
	return "Database works", nil
}

func (m *mockStorage) GetOrderByUID(ctx context.Context, orderUID string) (*models.Order, error ) {
	if orderUID == "exist" {
		return &models.Order{OrderUID: "exists"}, nil
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


// Тестирование подключения к базе 
func TestTestDBHandle(t *testing.T) {
	cfg := &config.Config{}
	cache := cache.NewCache(10)
	handler := NewHandler(&mockStorage{}, cfg, cache)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)


	handler.TestDBHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, but got %d", w.Code)
	}
}

// Тестирование получения заказа по UID из базы
func TestGetOrderByUIDHandle(t *testing.T) {
	cfg := &config.Config{}
	cache := cache.NewCache(10)
	handler := NewHandler(&mockStorage{}, cfg, cache)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Params = []gin.Param{{Key: "uid", Value: "exist"}}

	handler.GetOrderByUIDHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, but got %d", w.Code)
	}

	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Params = []gin.Param{{Key: "uid", Value: "nosuchorder"}}

	handler.GetOrderByUIDHandle(c)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, but got ")
	}
}

// Тестирование получения заказа по UID из кэша
func TestGetOrderByUIDHandleFromCache(t *testing.T) {
	cfg := &config.Config{}
	cache := cache.NewCache(10)
	handler := NewHandler(&mockStorage{}, cfg, cache)

	testOrder := &models.Order{OrderUID: "cached"}
	cache.Set(testOrder)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Params = []gin.Param{{Key: "uid", Value: "cached"}}
	
	handler.GetOrderByUIDHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, but got %d", w.Code)
	}
}