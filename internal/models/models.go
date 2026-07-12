package models

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/go-playground/validator/v10"
)

// Order represents a customer order with delivery, payment and items.
type Order struct {
	OrderUID          string    `json:"order_uid" validate:"required,uuid4"`
	TrackNumber       string    `json:"track_number" validate:"required,alphanum,max=50"`
	Entry             string    `json:"entry" validate:"required,alphanum,max=10"`
	Locale            string    `json:"locale" validate:"required,max=2"`
	InternalSignature string    `json:"internal_signature" validate:"omitempty,alphanum,max=100"`
	CustomerID        string    `json:"customer_id" validate:"required,alphanum,max=50"`
	DeliveryService   string    `json:"delivery_service" validate:"required,alphanum,max=50"`
	ShardKey          string    `json:"shardkey" validate:"required,alphanum,max=10"`
	SMID              uint      `json:"sm_id" validate:"required,min=1"`
	DateCreated       time.Time `json:"date_created" validate:"required"`
	OOFShard          string    `json:"oof_shard" validate:"required,alphanum,max=10"`
	Delivery          Delivery  `json:"delivery" validate:"required"`
	Payment           Payment   `json:"payment" validate:"required"`
	Items             []Item    `json:"items" validate:"required,min=1,dive"`
}

// Delivery holds shipping information for an order.
type Delivery struct {
	OrderUID string `json:"-"`
	Name     string `json:"name" validate:"required,max=100"`
	Phone    string `json:"phone" validate:"required,e164"`
	Zip      string `json:"zip" validate:"required,max=10"`
	City     string `json:"city" validate:"required,max=100"`
	Address  string `json:"address" validate:"required,max=100"`
	Region   string `json:"region" validate:"required,max=100"`
	Email    string `json:"email" validate:"required,email"`
}

// Payment holds transaction details for an order.
type Payment struct {
	OrderUID     string `json:"-"`
	Transaction  string `json:"transaction" validate:"required,uuid4"`
	RequestID    string `json:"request_id" validate:"omitempty,max=50"`
	Currency     string `json:"currency" validate:"required,max=3"`
	Provider     string `json:"provider" validate:"required,max=50"`
	Amount       int    `json:"amount" validate:"required,min=1"`
	PaymentDt    uint64 `json:"payment_dt" validate:"required"`
	Bank         string `json:"bank" validate:"required,alphanum,max=20"`
	DeliveryCost uint   `json:"delivery_cost" validate:"gte=0"`
	GoodsTotal   uint   `json:"goods_total" validate:"gte=0"`
	CustomFee    uint   `json:"custom_fee" validate:"gte=0"`
}

// Item represents a single product within an order.
type Item struct {
	ID          int    `json:"-"`
	OrderUID    string `json:"-"`
	ChrtID      uint   `json:"chrt_id" validate:"required,min=1"`
	TrackNumber string `json:"track_number" validate:"required,alphanum,max=50"`
	Price       uint   `json:"price" validate:"required,min=1"`
	Rid         string `json:"rid" validate:"required,alphanum,max=50"`
	Name        string `json:"name" validate:"required,max=50"`
	Sale        uint   `json:"sale" validate:"gte=0"`
	Size        string `json:"size" validate:"required,alphanum,max=10"`
	TotalPrice  uint   `json:"total_price" validate:"gte=0"`
	NmID        uint   `json:"nm_id" validate:"required,min=1"`
	Brand       string `json:"brand" validate:"required,max=50"`
	Status      uint   `json:"status" validate:"gte=0,max=999"`
}

// User represents a registered user stored in the database.
type User struct {
	ID           int    `json:"-"`
	Username     string `json:"-"`
	PasswordHash string `json:"-"`
	Role         string `json:"-"`
}

// LoginRequest is the JSON body for POST /login and POST /register.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// RefreshRequest is the JSON body for POST /refresh.
type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// LoadOrderFromFile reads and validates an Order from a JSON file.
func LoadOrderFromFile(path string) (*Order, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", path, err)
	}

	var order Order
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	validate := validator.New()
	if err := validate.Struct(order); err != nil {
		return nil, fmt.Errorf("failed to validate order: %v", err)
	}

	return &order, nil
}
