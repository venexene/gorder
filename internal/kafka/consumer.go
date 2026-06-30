package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"errors"

	"github.com/go-playground/validator/v10"
	"github.com/segmentio/kafka-go"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/database"
	"github.com/venexene/gorder/internal/models"
)

// Структура консьюмера
type Consumer struct {
	reader    *kafka.Reader
	storage   *database.Storage
	validator *validator.Validate
	cache	  *cache.Cache
}

// Конструктор консьюмера
func NewConsumer(brokers []string, topic string, storage *database.Storage, cache *cache.Cache) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic: topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
		MaxWait: time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		MaxAttempts: 3,

	})

	validate := validator.New()

	return &Consumer{
		reader: reader,
		storage: storage,
		validator: validate,
		cache: cache,
	}
}

// Основной метод для получения сообщений
func (c *Consumer) Consume(ctx context.Context) {
	for {
		msg, err := c.reader.ReadMessage(ctx) // Чтение сообщений из Kafka
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
                log.Println("Consumer: shutting down")
                return
            }
			log.Printf("Kafka failed to consume: %v", err)
			continue
		}
		log.Printf("Received message: %s", string(msg.Value))

		var order models.Order
		// Десериализация JSON
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}
		
		// Валидация структуры
		if err := c.validator.Struct(order); err != nil {
			log.Printf("Failed to validate: %v", err)
			continue
		}

		// Сохраниение в БД
		if err := c.storage.AddOrderIfNotExists(ctx, &order); err != nil {
			log.Printf("Failed to add order: %v", err)
		} else {
			log.Printf("Order saved with UID %s", order.OrderUID)
			c.cache.Set(&order) // Добавление в кэш
		}
	}
}

// Функция для закрытия соединения с Kafka
func (c *Consumer) Close() error {
	return c.reader.Close()
}