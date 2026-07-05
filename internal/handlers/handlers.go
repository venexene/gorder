package handlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/storage"
)

// Handler holds dependencies for HTTP request handlers.
type Handler struct {
	storage      storage.Interface
	cache        *cache.Cache
	logger       *slog.Logger
	kafkaBrokers string
}

// NewHandler creates a Handler with the given dependencies.
func NewHandler(storage storage.Interface, cache *cache.Cache, logger *slog.Logger, kafkaBrokers string) *Handler {
	return &Handler{
		storage:      storage,
		cache:        cache,
		logger:       logger,
		kafkaBrokers: kafkaBrokers,
	}
}

// HealthcheckHandle checks database and Kafka connectivity.
func (h *Handler) HealthcheckHandle(c *gin.Context) {
	var wg sync.WaitGroup
	var healthy atomic.Bool
	healthy.Store(true)

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := h.storage.CheckHealthDB(ctxDB); err != nil {
			healthy.Store(false)
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctxKafka, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		connKafka, err := kafka.DialContext(ctxKafka, "tcp", strings.Split(h.kafkaBrokers, ",")[0])
		if err != nil {
			healthy.Store(false)
			return
		}
		defer connKafka.Close()
	}()

	wg.Wait()

	if healthy.Load() {
		c.JSON(http.StatusOK, gin.H{"status": "UP"})
	} else {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "DOWN"})
	}
}

// GetOrderByUIDHandle returns full order data as JSON, using cache when available.
func (h *Handler) GetOrderByUIDHandle(c *gin.Context) {
	orderUID := c.Param("uid")
	if orderUID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No UID received",
		})
		return
	}

	if cachedOrder, exists := h.cache.Get(orderUID); exists {
		c.JSON(http.StatusOK, cachedOrder)
		return
	}

	order, err := h.storage.GetOrderByUID(c.Request.Context(), orderUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			h.logger.Warn("order not found", "order_uid", orderUID)
			c.JSON(http.StatusNotFound, gin.H{
				"error": "Failed to find order",
			})
		} else {
			h.logger.Error("failed to get info by uid", "error", err)
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
		h.logger.Error("failed to get uids", "error", err)
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
		h.logger.Error("failed to load orders", "error", err)
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
		h.logger.Error("failed to get info by uid", "error", err)
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
