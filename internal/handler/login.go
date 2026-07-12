package handler

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/venexene/gorder/internal/dto"
	"github.com/venexene/gorder/internal/models"
)

// LoginHandle authenticates a user by username and password, returning access and refresh tokens.
func (h *Handler) LoginHandle(c *gin.Context) {
	var login dto.LoginRequest

	if err := c.ShouldBindJSON(&login); err != nil {
		h.logger.Error("failed to bind json to struct", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("failed to bind json to struct: %s", err),
		})
		return
	}

	user, err := h.repo.GetUser(c.Request.Context(), login.Username)
	if err != nil {
		h.logger.Error("failed to get user from storage", "error", err)
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": fmt.Sprintf("failed to get user from storage: %s", err),
		})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(login.Password)); err != nil {
		h.logger.Error("failed to login", "error", err)
		c.JSON(http.StatusUnauthorized, gin.H{"error": fmt.Sprintf("failed to login: %s", err)})
		return
	}

	access, err := createToken(strconv.Itoa(user.ID), login.Username, user.Role, "access", 15*time.Minute, []byte(h.config.JWTSecret))
	if err != nil {
		h.logger.Error("failed to create access token", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to create access token: %s", err)})
		return
	}

	refresh, err := createToken(strconv.Itoa(user.ID), login.Username, user.Role, "refresh", 7*24*time.Hour, []byte(h.config.JWTSecret))
	if err != nil {
		h.logger.Error("failed to create refresh token", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to create refresh token: %s", err)})
		return
	}

	c.SetCookie("access_token", access, 900, "/", "", false, true)
	c.SetCookie("refresh_token", refresh, 604800, "/", "", false, true)
	c.JSON(http.StatusOK, gin.H{"status": "logged in"})
}

func (h *Handler) LoginPageHandle(c *gin.Context) {
	c.HTML(http.StatusOK, "login.html", nil)
}

func (h *Handler) LogoutHandle(c *gin.Context) {
	c.SetCookie("access_token", "", -1, "/", "", false, true)
	c.SetCookie("refresh_token", "", -1, "/", "", false, true)
	c.JSON(http.StatusOK, gin.H{"status": "logged out"})
}

// RegisterHandle creates a new user with a bcrypt-hashed password and default "user" role.
func (h *Handler) RegisterHandle(c *gin.Context) {
	var req dto.LoginRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("failed to bind json to struct: %s", err),
		})
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		h.logger.Error("faied to hash password", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to hash password: %s", err)})
		return
	}

	user := &models.User{
		Username:     req.Username,
		PasswordHash: string(hash),
		Role:         "user",
	}

	if err := h.repo.CreateUser(c.Request.Context(), user); err != nil {
		h.logger.Error("faied to create user in storage", "error", err)
		c.JSON(http.StatusConflict, gin.H{"error": fmt.Sprintf("failed to create user in storage: %s", err)})
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

// RefreshHandle validates a refresh token and returns a new pair of access and refresh tokens.
func (h *Handler) RefreshHandle(c *gin.Context) {
	var req dto.RefreshRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("failed to bind json to struct", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("failed to bind json to struct: %s", err),
		})
		return
	}

	token, err := jwt.Parse(req.RefreshToken, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(h.config.JWTSecret), nil
	})

	if err != nil || !token.Valid {
		h.logger.Error("invalid or expired token")
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Invalid or expired token",
		})
		return
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		h.logger.Error("invalid token claims")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid token claims",
		})
		return
	}

	userID, ok := claims["user_id"].(string)
	if !ok {
		h.logger.Error("invalid user id")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid user id",
		})
		return
	}

	username, ok := claims["username"].(string)
	if !ok {
		h.logger.Error("invalid username")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid username",
		})
		return
	}

	role, ok := claims["role"].(string)
	if !ok {
		h.logger.Error("invalid role")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid role",
		})
		return
	}

	newAccess, err := createToken(userID, username, role, "access", 15*time.Minute, []byte(h.config.JWTSecret))
	if err != nil {
		h.logger.Error("failed to create refresh token", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to create new access token: %s", err)})
		return
	}

	newRefresh, err := createToken(userID, username, role, "refresh", 7*24*time.Hour, []byte(h.config.JWTSecret))
	if err != nil {
		h.logger.Error("failed to create refresh token", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to create new refresh token: %s", err)})
		return
	}

	c.SetCookie("access_token", newAccess, 900, "/", "", false, true)
	c.SetCookie("refresh_token", newRefresh, 604800, "/", "", false, true)
	c.JSON(http.StatusOK, gin.H{"status": "logged in"})
}

func (h *Handler) RegisterPageHandle(c *gin.Context) {
	c.HTML(http.StatusOK, "register.html", nil)
}
