package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/venexene/gorder/internal/metrics"
)

// MetricsMiddleware records HTTP request duration as a Prometheus histogram
func MetricsMiddleware(m *metrics.Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		elapsed := time.Since(start).Seconds()
		m.RequestDuration.WithLabelValues(c.Request.Method, c.FullPath(), strconv.Itoa(c.Writer.Status())).Observe(elapsed)
	}
}
