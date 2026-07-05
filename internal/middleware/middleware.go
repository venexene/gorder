package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	ginprom "github.com/logocomune/gin-prometheus"

	"github.com/venexene/gorder/internal/metrics"
)

// All returns all middleware functions for the Gin router.
func All(m *metrics.Metrics) []gin.HandlerFunc {
	return []gin.HandlerFunc{
		MetricsMiddleware(m),
		ginprom.Middleware(),
	}
}

// MetricsMiddleware records HTTP request duration as a Prometheus histogram
func MetricsMiddleware(m *metrics.Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		elapsed := time.Since(start).Seconds()
		m.RequestDuration.WithLabelValues(c.Request.Method, c.FullPath(), strconv.Itoa(c.Writer.Status())).Observe(elapsed)
	}
}
