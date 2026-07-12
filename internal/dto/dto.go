// Package dto defines data transfer objects for API requests and responses.
package dto

// LoginRequest is the JSON body for POST /login and POST /register.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// RefreshRequest is the JSON body for POST /refresh.
type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}
