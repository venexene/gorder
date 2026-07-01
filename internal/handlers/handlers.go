package handlers

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/database"
)

// Handler holds dependencies for HTTP request handlers.
type Handler struct {
	storage database.StorageInterface
	cache   *cache.Cache
	kafkaBrokers string
}

// NewHandler creates a Handler with the given dependencies.
func NewHandler(storage database.StorageInterface, cache *cache.Cache, kafkaBrokers string) *Handler {
	return &Handler{
		storage: storage,
		cache:   cache,
		kafkaBrokers: kafkaBrokers,
	}
}

// TestServerHandle responds with a simple server status check.
func (h *Handler) TestServerHandle(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "Server definetly works",
	})
}

// TestDBHandle verifies the database connection and returns its status.
func (h *Handler) TestDBHandle(c *gin.Context) {
	res, err := h.storage.TestDB(c.Request.Context())
	if err != nil {
		log.Printf("Failed to test database: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to connect database",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": res,
	})
}

// TestKafkaHandle checks connectivity to the Kafka broker.
func (h *Handler) TestKafkaHandle(c *gin.Context) {
	ctxKafka, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	connKafka, err := kafka.DialContext(ctxKafka, "tcp", h.kafkaBrokers)
	if err != nil {
		log.Printf("Failed to test Kafka: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to connect Kafka",
		})
		return
	}
	defer connKafka.Close()

	broker := connKafka.Broker()
	c.JSON(http.StatusOK, gin.H{
		"status":    "Kafka definetly works",
		"brokers":   h.kafkaBrokers,
		"broker_id": broker.ID,
	})
}

// GetOrderByUIDHandle returns full order data as JSON, using cache when available.
func (h *Handler) GetOrderByUIDHandle(c *gin.Context) {
	orderUID := c.Param("uid")
	if orderUID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No UID recieved",
		})
		return
	}

	if cachedOrder, exists := h.cache.Get(orderUID); exists {
		c.JSON(http.StatusOK, cachedOrder)
		return
	}

	order, err := h.storage.GetOrderByUID(c.Request.Context(), orderUID)
	if err != nil {
		log.Printf("Failed to get info by UID: %v", err)
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "Failed to find order",
			})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Internal server error",
			})
		}
		return
	}

	h.cache.Set(order)
	c.JSON(http.StatusOK, order)
}

// GetAllOrdersUIDHandle returns all order UIDs as JSON.
func (h *Handler) GetAllOrdersUIDHandle(c *gin.Context) {
	orderUIDs, err := h.storage.GetAllOrdersUID(c.Request.Context())

	if err != nil {
		log.Printf("Failed to get UIDs: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to get order UIDs",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"order_uids": orderUIDs,
	})
}

// AllOrdersPageHandle renders the HTML page listing all orders.
func (h *Handler) AllOrdersPageHandle(c *gin.Context) {
	orderUIDs, err := h.storage.GetAllOrdersUID(c.Request.Context())

	if err != nil {
		log.Printf("Failed to get UIDs: %v", err)
		c.HTML(http.StatusInternalServerError, "error.html", gin.H{
			"error": "Failed to load orders",
		})
		return
	}

	c.HTML(http.StatusOK, "orders.html", gin.H{
		"orders": orderUIDs,
	})
}

// OrderPageHandle renders the HTML page for a single order.
func (h *Handler) OrderPageHandle(c *gin.Context) {
	orderUID := c.Param("uid")

	if orderUID == "" {
		c.HTML(http.StatusBadRequest, "error.html", gin.H{
			"error": "No UID received",
		})
		return
	}

	if cachedOrder, exists := h.cache.Get(orderUID); exists {
		c.HTML(http.StatusOK, "order.html", cachedOrder)
		return
	}

	order, err := h.storage.GetOrderByUID(c.Request.Context(), orderUID)
	if err != nil {
		log.Printf("Failed to get info by UID: %v", err)
		if errors.Is(err, pgx.ErrNoRows) {
			c.HTML(http.StatusNotFound, "error.html", gin.H{
				"error": "Orders not found",
			})
		} else {
			c.HTML(http.StatusInternalServerError, "error.html", gin.H{
				"error": "Internal server error",
			})
		}
		return
	}

	h.cache.Set(order)
	c.HTML(http.StatusOK, "order.html", order)
}
