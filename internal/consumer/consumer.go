package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/segmentio/kafka-go"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/metrics"
	"github.com/venexene/gorder/internal/models"
	"github.com/venexene/gorder/internal/storage"
)

// MessageReader abstracts reading messages from a message broker.
type MessageReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

// Consumer reads orders from topic and persists them.
type Consumer struct {
	reader    	 MessageReader
	storage   	 storage.Interface
	validator 	 *validator.Validate
	cache     	 *cache.Cache
	logger    	 *slog.Logger
	metrics   	 *metrics.Metrics
	kafkaBrokers string 
}

// NewConsumer creates a Consumer.
func NewConsumer(reader MessageReader, storage storage.Interface, cache *cache.Cache, logger *slog.Logger, metrics *metrics.Metrics, kafkaBrokers string) *Consumer {
	validate := validator.New()

	return &Consumer{
		reader:    reader,
		storage:   storage,
		validator: validate,
		cache:     cache,
		logger:    logger,
		metrics:   metrics,
		kafkaBrokers: kafkaBrokers,
	}
}

// Consume reading messages from Kafka in a blocking loop.
func (c *Consumer) Consume(ctx context.Context) {
	for {
		msg, err := c.readMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.logger.Info("consumer shutting down")
				return
			}
			c.logger.Error("failed to consume", "error", err)
			continue
		}
		c.logger.Debug("received message", "message", string(msg.Value))

		order, err := c.processMessage(msg)
		if err != nil {
			c.logger.Error("failed to process message", "error", err)
			continue
		}

		if err := c.storage.AddOrderIfNotExists(ctx, order); err != nil {
			c.logger.Error("failed to add order", "error", err)
		} else {
			c.logger.Info("order saved", "order_uid", order.OrderUID)
			c.cache.Set(order)
			if c.metrics != nil {
				c.metrics.OrdersProcessed.Add(1)
			}
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

// Close shuts down consumer reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) CheckHealth(ctx context.Context) error {
	_, err := kafka.DialContext(ctx, "tcp", strings.Split(c.kafkaBrokers, ",")[0])
	return err
}

type HealthChecker interface {
	CheckHealth(ctx context.Context) error
}

