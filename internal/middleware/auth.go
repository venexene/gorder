package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

func JWTAuth(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		auth := c.GetHeader("Authorization")

		typ, tokenStr, found := strings.Cut(auth, " ")
		if !found || !strings.EqualFold(typ, "Bearer") {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "Authorization header must be in format: Bearer <token>",
			})
			c.Abort()
			return
		}

		token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return []byte(secret), nil
		})

		if err != nil || !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "Invalid or expired token",
			})
			c.Abort()
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
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
