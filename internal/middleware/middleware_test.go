package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"

	"github.com/venexene/gorder/internal/metrics"
)

const testSecret = "test-jwt-secret"

func genTestToken(claims jwt.MapClaims, secret string) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, _ := token.SignedString([]byte(secret))
	return signed
}

func setupJWTTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(JWTAuth(testSecret))
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
	return router
}

// TestMetricsMiddleware_ChainWorks verifies the middleware does not break the request chain.
func TestMetricsMiddleware_ChainWorks(t *testing.T) {
	m := metrics.NewMetrics()
	router := gin.New()
	router.Use(MetricsMiddleware(m))
	router.GET("/test", func(c *gin.Context) {
		c.String(200, "ok")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// TestJWTAuth_ValidToken verifies 200 with a correctly signed token.
func TestJWTAuth_ValidToken(t *testing.T) {
	router := setupJWTTestRouter()

	token := genTestToken(jwt.MapClaims{
		"user_id":  "test-user",
		"username": "tester",
		"role":     "admin",
		"exp":      time.Now().Add(time.Hour).Unix(),
	}, testSecret)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// TestJWTAuth_MissingHeader verifies 401 when Authorization header is absent.
func TestJWTAuth_MissingHeader(t *testing.T) {
	router := setupJWTTestRouter()

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

// TestJWTAuth_InvalidSignature verifies 401 with a token signed by a different secret.
func TestJWTAuth_InvalidSignature(t *testing.T) {
	router := setupJWTTestRouter()

	token := genTestToken(jwt.MapClaims{
		"user_id": "test-user",
	}, "wrong-secret")

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

// TestJWTAuth_ExpiredToken verifies 401 with an expired token.
func TestJWTAuth_ExpiredToken(t *testing.T) {
	router := setupJWTTestRouter()

	token := genTestToken(jwt.MapClaims{
		"user_id": "test-user",
		"exp":     time.Now().Add(-time.Hour).Unix(),
	}, testSecret)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

// TestJWTAuth_NoBearerPrefix verifies 401 when token lacks Bearer prefix.
func TestJWTAuth_NoBearerPrefix(t *testing.T) {
	router := setupJWTTestRouter()

	token := genTestToken(jwt.MapClaims{
		"user_id": "test-user",
	}, testSecret)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", token) // без Bearer
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}
