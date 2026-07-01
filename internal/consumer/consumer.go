package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/go-playground/validator/v10"
	"github.com/segmentio/kafka-go"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/database"
	"github.com/venexene/gorder/internal/models"
)

// MessageReader abstracts reading messages from a message broker.
type MessageReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

// Consumer reads orders from a Kafka topic and persists them.
type Consumer struct {
	reader    MessageReader
	storage   database.StorageInterface
	validator *validator.Validate
	cache     *cache.Cache
}

// NewConsumer creates a Consumer with the given reader, storage, and cache.
func NewConsumer(reader MessageReader, storage database.StorageInterface, cache *cache.Cache) *Consumer {

	validate := validator.New()

	return &Consumer{
		reader:    reader,
		storage:   storage,
		validator: validate,
		cache:     cache,
	}
}

// Consume starts reading messages from Kafka in a blocking loop until the context is cancelled.
func (c *Consumer) Consume(ctx context.Context) {
	for {
		msg, err := c.readMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				log.Println("Consumer: shutting down")
				return
			}
			log.Printf("Kafka failed to consume: %v", err)
			continue
		}
		log.Printf("Received message: %s", string(msg.Value))

		order, err := c.processMessage(msg)

		if err != nil {
			log.Printf("Failed to process message: %v", err)
			continue
		}

		if err := c.storage.AddOrderIfNotExists(ctx, order); err != nil {
			log.Printf("Failed to add order: %v", err)
		} else {
			log.Printf("Order saved with UID %s", order.OrderUID)
			c.cache.Set(order)
		}
	}
}

func (c *Consumer) readMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return kafka.Message{}, err
	}
	return msg, nil
}

func (c *Consumer) processMessage(msg kafka.Message) (*models.Order, error) {
	order := &models.Order{}

	if err := json.Unmarshal(msg.Value, order); err != nil {
		return nil, err
	}

	if err := c.validator.Struct(order); err != nil {
		return nil, err
	}

	return order, nil
}

// Close shuts down Kafka reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
