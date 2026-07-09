package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/venexene/gorder/internal/models"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:9092"
	}
	kafkaBrokers := strings.Split(brokers, ",")

	topic := "wbl0_orders"
	writer := &kafka.Writer{
		Addr:  kafka.TCP(kafkaBrokers...),
		Topic: topic,
	}
	defer writer.Close()
	for i := 0; i < 10; i++ {
		order := generateRandomOrder()
		message, err := json.Marshal(order)
		if err != nil {
			logger.Error("failed to marshal order", "error", err)
			continue
		}
		err = writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(order.OrderUID),
				Value: message,
			},
		)

		if err != nil {
			logger.Error("failed to write message", "error", err)
			os.Exit(1)
		} else {
			logger.Debug("message sent", "order_uid", order.OrderUID)
		}

		time.Sleep(1 * time.Second)
	}
	for i := 1; i < 6; i++ {
		filename := filepath.Join("testdata", fmt.Sprintf("order%d.json", i))
		order, err := models.LoadOrderFromFile(filename)
		if err != nil {
			logger.Error("failed to load order from file", "error", err)
			continue
		}
		message, err := json.Marshal(order)
		if err != nil {
			logger.Error("failed to marshal order", "error", err)
			continue
		}
		err = writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(order.OrderUID),
				Value: message,
			},
		)

		if err != nil {
			logger.Error("failed to write message", "error", err)
			os.Exit(1)
		} else {
			logger.Debug("message sent", "order_uid", order.OrderUID)
		}

		time.Sleep(1 * time.Second)
	}
}

// generateRandomOrder creates an order with randomized fields for testing.
func generateRandomOrder() models.Order {
	orderUID := uuid.New().String()
	currentTime := time.Now()

	delivery := models.Delivery{
		OrderUID: orderUID,
		Name:     generateRandomString(100),
		Phone:    generateRandomPhone(),
		Zip:      fmt.Sprintf("%d", rand.IntN(100000)),
		City:     generateRandomString(20),
		Address:  generateRandomString(20),
		Region:   generateRandomString(20),
		Email:    generateRandomEmail(),
	}

	payment := models.Payment{
		OrderUID:     orderUID,
		Transaction:  orderUID,
		RequestID:    generateRandomString(10),
		Currency:     "USD",
		Provider:     "WBPAY",
		Amount:       rand.IntN(100000) + 10,
		PaymentDt:    uint64(currentTime.Unix()) - rand.Uint64N(1000000),
		Bank:         generateRandomString(15),
		DeliveryCost: rand.UintN(1000),
		GoodsTotal:   rand.UintN(500),
		CustomFee:    rand.UintN(100),
	}

	itemsCount := rand.IntN(5) + 1
	var items []models.Item
	for i := 0; i < itemsCount; i++ {
		item := models.Item{
			OrderUID:    orderUID,
			ChrtID:      rand.UintN(100000) + 1,
			TrackNumber: generateRandomString(10),
			Price:       rand.UintN(100000),
			Rid:         generateRandomString(20),
			Name:        generateRandomString(15),
			Sale:        rand.UintN(99),
			Size:        fmt.Sprintf("%d", rand.UintN(5)+1),
			TotalPrice:  rand.UintN(100000),
			NmID:        rand.UintN(100000) + 1,
			Brand:       generateRandomString(12),
			Status:      rand.UintN(999),
		}
		items = append(items, item)
	}

	order := models.Order{
		OrderUID:          orderUID,
		TrackNumber:       generateRandomString(10),
		Entry:             generateRandomString(4),
		Locale:            "ru",
		InternalSignature: generateRandomString(10),
		CustomerID:        generateRandomString(8),
		DeliveryService:   generateRandomString(7),
		ShardKey:          fmt.Sprintf("%d", rand.IntN(10)),
		SMID:              rand.UintN(100) + 1,
		DateCreated:       currentTime,
		OOFShard:          fmt.Sprintf("%d", rand.IntN(10)),
		Delivery:          delivery,
		Payment:           payment,
		Items:             items,
	}

	return order
}

// generateRandomString returns a random alphanumeric string of the given length.
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.IntN(len(charset))]
	}
	return string(b)
}

// generateRandomPhone returns a random Russian phone number.
func generateRandomPhone() string {
	return fmt.Sprintf("+7%d", rand.IntN(1000000000))
}

// generateRandomEmail returns a random email address.
func generateRandomEmail() string {
	return fmt.Sprintf("%s@%s.com", generateRandomString(10), generateRandomString(5))
}
