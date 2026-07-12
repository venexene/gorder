// Package middleware provides HTTP middleware for JWT auth, role-based access, and metrics.
package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// JWTAuth validates a JWT from the Authorization header and sets user claims into the Gin context.
func JWTAuth(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenStr := ""
		var err error

		auth := c.GetHeader("Authorization")
		if strings.HasPrefix(auth, "Bearer") {
			tokenStr = strings.TrimPrefix(auth, "Bearer ")
		}

		if tokenStr == "" {
			tokenStr, err = c.Cookie("access_token")
			if err != nil {
				if strings.Contains(c.GetHeader("Accept"), "text/html") {
					c.Redirect(http.StatusFound, "/login")
					c.Abort()
					return
				}
				c.JSON(http.StatusUnauthorized, gin.H{
					"error": "Failed to read access token from cookie",
				})
				c.Abort()
				return
			}
		}

		token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return []byte(secret), nil
		})

		if err != nil || !token.Valid {
			if strings.Contains(c.GetHeader("Accept"), "text/html") {
				c.Redirect(http.StatusFound, "/login")
				c.Abort()
				return
			}
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "Invalid or expired token",
			})
			c.Abort()
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			if strings.Contains(c.GetHeader("Accept"), "text/html") {
				c.Redirect(http.StatusFound, "/login")
				c.Abort()
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Invalid token claims",
			})
			c.Abort()
			return
		}

		c.Set("user_id", claims["user_id"])
		c.Set("username", claims["username"])
		c.Set("role", claims["role"])
		c.Set("claims", claims)

		c.Next()
	}
}

// RequireRole restricts access to users whose role matches one of the given roles.
func RequireRole(roles ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		r, exists := c.Get("role")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "role not found"})
			c.Abort()
			return
		}

		role, ok := r.(string)
		if !ok {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid role type"})
			c.Abort()
			return
		}

		access := false
		for _, r := range roles {
			if role == r {
				access = true
			}
		}

		if !access {
			c.JSON(http.StatusForbidden, gin.H{"error": "access denied"})
			c.Abort()
			return
		}

		c.Next()
	}
}
