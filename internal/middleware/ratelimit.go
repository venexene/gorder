package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ulule/limiter/v3"
)

// RateLimitMiddleware blocks requests exceeding the configured rate limit.
func RateLimitMiddleware(limit *limiter.Limiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()

		ctx, err := limit.Get(c.Request.Context(), ip)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get rate limiter"})
			c.Abort()
			return
		}

		if ctx.Reached {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "too many requests"})
			c.Abort()
			return
		}

		c.Next()
	}
}
