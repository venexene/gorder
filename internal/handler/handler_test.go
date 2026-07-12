package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/config"
	"github.com/venexene/gorder/internal/models"
)

const testJWTSecret = "test-secret-for-jwt"

func setTestUser(c *gin.Context, userID string) {
	c.Set("user_id", userID)
}

type mockStorage struct {
	healthError   error
	users         map[string]*models.User
	createUserErr error
}

func newMockStorage() *mockStorage {
	hash, _ := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
	return &mockStorage{
		users: map[string]*models.User{
			"testuser": {ID: 1, Username: "testuser", PasswordHash: string(hash), Role: "user"},
			"admin":    {ID: 2, Username: "admin", PasswordHash: string(hash), Role: "admin"},
		},
	}
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
	if m.createUserErr != nil {
		return m.createUserErr
	}
	if _, exists := m.users[user.Username]; exists {
		return fmt.Errorf("duplicate user: %s", user.Username)
	}
	m.users[user.Username] = user
	return nil
}

func (m *mockStorage) GetUser(ctx context.Context, username string) (*models.User, error) {
	user, exists := m.users[username]
	if !exists {
		return nil, fmt.Errorf("failed to find user with username %s: %w", username, pgx.ErrNoRows)
	}
	return user, nil
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
		Repository: newMockStorage(),
		Consumer:   &mockConsumer{},
		Cache:      cache.NewCache(10, logger, nil),
		Logger:     logger,
		Config:     &config.Config{JWTSecret: testJWTSecret},
	}
	return NewHandler(hd)
}

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

func TestReadyCheckHandle(t *testing.T) {
	tests := []struct {
		name       string
		dbError    error
		kafkaError error
		wantCode   int
		wantStatus string
	}{
		{
			name:       "all healthy",
			dbError:    nil,
			kafkaError: nil,
			wantCode:   http.StatusOK,
			wantStatus: "UP",
		},
		{
			name:       "kafka down",
			dbError:    nil,
			kafkaError: fmt.Errorf("kafka down"),
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "DOWN",
		},
		{
			name:       "db down",
			dbError:    fmt.Errorf("db down"),
			kafkaError: nil,
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "DOWN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestHandler()
			handler.consumer = &mockConsumer{healthError: tt.kafkaError}
			handler.repo = &mockStorage{healthError: tt.dbError}

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("GET", "/health", nil)

			handler.ReadyCheckHandle(c)

			if w.Code != tt.wantCode {
				t.Errorf("expected %d, got %d", tt.wantCode, w.Code)
			}

			var body map[string]string
			json.NewDecoder(w.Body).Decode(&body)
			if body["status"] != tt.wantStatus {
				t.Errorf("expected status %s, got %s", tt.wantStatus, body["status"])
			}
		})
	}
}

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

func TestLoginHandle(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "successful login",
			body:     `{"username":"testuser","password":"password"}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "wrong password",
			body:     `{"username":"testuser","password":"wrongpass"}`,
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "non-existent user",
			body:     `{"username":"nobody","password":"password"}`,
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "invalid json",
			body:     `{bad`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestHandler()
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("POST", "/login",
				strings.NewReader(tt.body))
			c.Request.Header.Set("Content-Type", "application/json")

			handler.LoginHandle(c)

			if w.Code != tt.wantCode {
				t.Errorf("expected %d, got %d", tt.wantCode, w.Code)
			}

			if tt.wantCode == http.StatusOK {
				var body map[string]string
				json.NewDecoder(w.Body).Decode(&body)
				if body["status"] != "logged in" {
					t.Errorf("expected status 'logged in', got %s", body["status"])
				}

				cookies := w.Result().Cookies()
				if !hasCookie(cookies, "access_token") {
					t.Error("expected access_token cookie")
				}
				if !hasCookie(cookies, "refresh_token") {
					t.Error("expected refresh_token cookie")
				}
			}
		})
	}
}

func TestLogoutHandle(t *testing.T) {
	handler := newTestHandler()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/logout", nil)

	handler.LogoutHandle(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	cookies := w.Result().Cookies()
	for _, ck := range cookies {
		if ck.Name == "access_token" || ck.Name == "refresh_token" {
			if ck.MaxAge != -1 {
				t.Errorf("expected %s cookie to be cleared (MaxAge=-1)", ck.Name)
			}
		}
	}
}

func hasCookie(cookies []*http.Cookie, name string) bool {
	for _, c := range cookies {
		if c.Name == name && c.Value != "" {
			return true
		}
	}
	return false
}

func TestRegisterHandle(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "successful registration",
			body:     `{"username":"newuser","password":"secret"}`,
			wantCode: http.StatusCreated,
		},
		{
			name:     "duplicate username",
			body:     `{"username":"testuser","password":"secret"}`,
			wantCode: http.StatusConflict,
		},
		{
			name:     "invalid json",
			body:     `{bad`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestHandler()
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("POST", "/register",
				strings.NewReader(tt.body))
			c.Request.Header.Set("Content-Type", "application/json")

			handler.RegisterHandle(c)

			if w.Code != tt.wantCode {
				t.Errorf("expected %d, got %d", tt.wantCode, w.Code)
			}

			if tt.wantCode == http.StatusCreated {
				var body map[string]string
				json.NewDecoder(w.Body).Decode(&body)
				if body["status"] != "created" {
					t.Errorf("expected status 'created', got %s", body["status"])
				}
			}
		})
	}
}

func TestRefreshHandle(t *testing.T) {
	tests := []struct {
		name        string
		refreshJSON string
		wantCode    int
	}{
		{
			name:        "valid refresh",
			refreshJSON: `{"refresh_token":"` + makeRefreshToken("testuser", "user") + `"}`,
			wantCode:    http.StatusOK,
		},
		{
			name:        "expired refresh",
			refreshJSON: `{"refresh_token":"` + makeExpiredRefreshToken("testuser", "user") + `"}`,
			wantCode:    http.StatusUnauthorized,
		},
		{
			name:        "invalid json",
			refreshJSON: `{bad`,
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestHandler()
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("POST", "/refresh",
				strings.NewReader(tt.refreshJSON))
			c.Request.Header.Set("Content-Type", "application/json")

			handler.RefreshHandle(c)

			if w.Code != tt.wantCode {
				t.Errorf("expected %d, got %d", tt.wantCode, w.Code)
			}

			if tt.wantCode == http.StatusOK {
				var body map[string]string
				json.NewDecoder(w.Body).Decode(&body)
				if body["status"] != "logged in" {
					t.Errorf("expected status 'logged in', got %s", body["status"])
				}

				cookies := w.Result().Cookies()
				if !hasCookie(cookies, "access_token") {
					t.Error("expected access_token cookie")
				}
				if !hasCookie(cookies, "refresh_token") {
					t.Error("expected refresh_token cookie")
				}
			}
		})
	}
}

func makeRefreshToken(username, role string) string {
	token, _ := createToken("42", username, role, "refresh", 7*24*time.Hour, []byte(testJWTSecret))
	return token
}

func makeExpiredRefreshToken(username, role string) string {
	token, _ := createToken("42", username, role, "refresh", -1*time.Hour, []byte(testJWTSecret))
	return token
}
