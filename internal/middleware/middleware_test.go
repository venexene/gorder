package middleware

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/venexene/gorder/internal/metrics"
)

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
