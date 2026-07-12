// Binary gen-token generates a JWT for testing purposes.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Overload(".env"); err != nil {
		if !os.IsNotExist(err) {
			slog.Error("failed to load .env file")
			os.Exit(1)
		}
		slog.Warn(".env file not found, using OS environment variables")
	}

	JWTSecret := os.Getenv("JWT_SECRET")
	if JWTSecret == "" {
		slog.Error("JWT_SECRET is require")
		os.Exit(1)
	}

	claims := jwt.MapClaims{
		"sub":      "test-user",
		"iat":      time.Now().Unix(),
		"exp":      time.Now().Add(24 * time.Hour).Unix(),
		"user_id":  "test-user",
		"username": "tester",
		"role":     "admin",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signedToken, err := token.SignedString([]byte(JWTSecret))
	if err != nil {
		slog.Error("failed to sign token")
		os.Exit(1)
	}

	fmt.Printf("Token: %s\n", signedToken)
}
