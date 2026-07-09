package handlers

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/venexene/gorder/internal/models"
)

func (h *Handler) LoginHandle(c *gin.Context) {
	var login models.LoginRequest

	if err := c.ShouldBindJSON(&login); err != nil {
		h.logger.Error("failed to bind json to struct", "error", err)
		c.JSON(http.StatusBadRequest, gin.H {
			"error": fmt.Sprintf("failed to bind json to struct: %s", err),
		})
		return
	}

	user, err := h.storage.GetUser(c.Request.Context(), login.Username)
	if err != nil {
		h.logger.Error("failed to get user from storage", "error", err)
		c.JSON(http.StatusUnauthorized, gin.H {
			"error": fmt.Sprintf("failed to get user from storage: %s", err),
		})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(login.Password)); err != nil {
		h.logger.Error("failed to login", "error", err)
		c.JSON(http.StatusUnauthorized, gin.H {"error": fmt.Sprintf("failed to login: %s", err)})
		return
	}

	access, err := createToken(strconv.Itoa(user.ID), login.Username, user.Role, "access", 15 * time.Minute, []byte(h.config.JWTSecret))
	if err != nil {
		h.logger.Error("failed to create access token", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H {"error": fmt.Sprintf("failed to create access token: %s", err)})
		return
	}

	refresh, err := createToken(strconv.Itoa(user.ID), login.Username, user.Role, "refresh", 7 * 24 * time.Hour, []byte(h.config.JWTSecret))
	if err != nil {
		h.logger.Error("failed to create refresh token", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H {"error": fmt.Sprintf("failed to create refresh token: %s", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H {
		"access_token": access,
		"refresh_token": refresh,
	})
}

func (h *Handler) RegisterHandle(c *gin.Context) {
	var login models.LoginRequest

	if err := c.ShouldBindJSON(&login); err != nil {
		c.JSON(http.StatusBadRequest, gin.H {
			"error": fmt.Sprintf("failed to bind json to struct: %s", err),
		})
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(login.Password), bcrypt.DefaultCost)
	if err != nil {
		h.logger.Error("faied to hash password", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H {"error": fmt.Sprintf("failed to hash password: %s", err)})
		return
	}

	user := &models.User{
		Username: login.Username,
		PasswordHash: string(hash),
		Role: "user",
	}

	if err := h.storage.CreateUser(c.Request.Context(), user); err != nil {
		h.logger.Error("faied to create user in storage", "error", err)
		c.JSON(http.StatusConflict, gin.H {"error": fmt.Sprintf("failed to create user in storage: %s", err)})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

func createToken(userID, username, role, tokenType string, ttl time.Duration, secret []byte) (string, error) {
	claims := jwt.MapClaims{
		"user_id":  userID,
    	"username": username,
        "role":     role,
        "type":     tokenType,
        "iat":      time.Now().Unix(),
        "exp":      time.Now().Add(ttl).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}