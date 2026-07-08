package handlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/consumer"
	"github.com/venexene/gorder/internal/storage"
)

const (
	statusDown = "DOWN"
	statusUp = "UP"
)

// Handler holds dependencies for HTTP request handlers.
type Handler struct {
	storage  storage.Interface
	consumer consumer.HealthChecker
	cache    *cache.Cache
	logger   *slog.Logger
}

// NewHandler creates a Handler with the given dependencies.
func NewHandler(storage storage.Interface, consumer consumer.HealthChecker, cache *cache.Cache, logger *slog.Logger) *Handler {
	return &Handler{
		storage:  storage,
		consumer: consumer,
		cache:    cache,
		logger:   logger,
	}
}

// LiveCheckHandle checks service liveness.
func (h *Handler) LiveCheckHandle(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": statusUp})
}

// ReadyCheckHandle checks database and Kafka connectivity.
func (h *Handler) ReadyCheckHandle(c *gin.Context) {
	var wg sync.WaitGroup
	var healthy atomic.Bool
	healthy.Store(true)
	dbStatus := "OK"
	consumerStatus := "OK"

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := h.storage.CheckHealthDB(ctxDB); err != nil {
			healthy.Store(false)
			dbStatus = statusDown
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctxKafka, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := h.consumer.CheckHealth(ctxKafka); err != nil {
			healthy.Store(false)
			consumerStatus = statusDown
			return
		}
	}()

	wg.Wait()

	if healthy.Load() {
		c.JSON(http.StatusOK, gin.H{
			"status": statusUp,
			"db":     dbStatus,
			"kafka":  consumerStatus,
		})
	} else {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": statusDown,
			"db":     dbStatus,
			"kafka":  consumerStatus,
		})
	}
}

// GetOrderByUIDHandle returns full order data as JSON, using cache when available.
func (h *Handler) GetOrderByUIDHandle(c *gin.Context) {
	userID, ok := c.Get("user_id")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User is not logged",
		})
		return
	}
	userLogger := h.logger.With("user_id", userID)

	orderUID := c.Param("uid")
	if orderUID == "" {
		userLogger.Warn("no uid received")
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
			userLogger.Warn("order not found", "order_uid", orderUID)
			c.JSON(http.StatusNotFound, gin.H{
				"error": "Failed to find order",
			})
		} else {
			userLogger.Error("failed to get info by uid", "error", err)
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
	userID, ok := c.Get("user_id")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User is not logged",
		})
		return
	}
	userLogger := h.logger.With("user_id", userID)

	orderUIDs, err := h.storage.GetAllOrdersUID(c.Request.Context())
	if err != nil {
		userLogger.Error("failed to get uids", "error", err)
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
