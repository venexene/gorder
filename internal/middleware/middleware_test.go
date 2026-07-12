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

func TestJWTAuth_MissingHeader(t *testing.T) {
	router := setupJWTTestRouter()

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

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

func TestJWTAuth_CookieFallback(t *testing.T) {
	router := setupJWTTestRouter()

	token := genTestToken(jwt.MapClaims{
		"user_id":  "test-user",
		"username": "tester",
		"role":     "admin",
		"exp":      time.Now().Add(time.Hour).Unix(),
	}, testSecret)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	req.AddCookie(&http.Cookie{Name: "access_token", Value: token})
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with cookie auth, got %d", w.Code)
	}
}

func TestJWTAuth_BrowserRedirect(t *testing.T) {
	router := setupJWTTestRouter()
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Accept", "text/html")

	router.ServeHTTP(w, req)

	if w.Code != http.StatusFound {
		t.Errorf("expected 302 redirect, got %d", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "/login" {
		t.Errorf("expected redirect to /login, got %s", loc)
	}
}
func setupRequireRoleRouter(roles ...string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("role", "user")
		c.Next()
	})
	router.Use(RequireRole(roles...))
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
	return router
}

func TestRequireRole_Allowed(t *testing.T) {
	router := setupRequireRoleRouter("user")

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for allowed role, got %d", w.Code)
	}
}

func TestRequireRole_MultipleRoles(t *testing.T) {
	router := setupRequireRoleRouter("admin", "user", "moderator")

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 when role in allowed list, got %d", w.Code)
	}
}

func TestRequireRole_Forbidden(t *testing.T) {
	router := setupRequireRoleRouter("admin")

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403 for disallowed role, got %d", w.Code)
	}
}

func TestRequireRole_NoRoleInContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequireRole("admin"))
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 when role not in context, got %d", w.Code)
	}
}

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
