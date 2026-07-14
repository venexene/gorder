// Package handler implements HTTP request handlers for the gorder service.
package handler

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
	"github.com/venexene/gorder/internal/config"
	"github.com/venexene/gorder/internal/consumer"
	"github.com/venexene/gorder/internal/models"
	"github.com/venexene/gorder/internal/repository"
)

const (
	statusDown = "DOWN"
	statusUp   = "UP"
)

type HandlerDependencies struct {
	Repository repository.Interface
	Consumer   consumer.HealthChecker
	Cache      *cache.Cache
	Logger     *slog.Logger
	Config     *config.Config
	Version    string
	Commit     string
}

// Handler holds dependencies for HTTP request handlers.
type Handler struct {
	repo     repository.Interface
	consumer consumer.HealthChecker
	cache    *cache.Cache
	logger   *slog.Logger
	config   *config.Config
	version  string
	commit   string
}

// NewHandler creates a Handler with the given dependencies.
func NewHandler(hd *HandlerDependencies) *Handler {
	return &Handler{
		repo:     hd.Repository,
		consumer: hd.Consumer,
		cache:    hd.Cache,
		logger:   hd.Logger,
		config:   hd.Config,
		version:  hd.Version,
		commit:   hd.Commit,
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
		if err := h.repo.CheckHealthDB(ctxDB); err != nil {
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
// @Summary      Get order by UID (admin)
// @Description  Returns full order data as JSON for admin users.
// @Tags         orders
// @Security     BearerAuth
// @Produce      json
// @Param        uid  path  string  true  "Order UID"
// @Success      200  {object}  models.Order
// @Failure      401  {object}  map[string]string
// @Failure      404  {object}  map[string]string
// @Router       /api/admin/orders/{uid} [get]
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

	order, err := h.repo.GetOrderByUID(c.Request.Context(), orderUID)
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

// GetUserOrderByUIDHandle returns a user's own order as JSON.
// @Summary      Get own order by UID (user)
// @Description  Returns order data if it belongs to the authenticated user.
// @Tags         orders
// @Security     BearerAuth
// @Produce      json
// @Param        uid  path  string  true  "Order UID"
// @Success      200  {object}  models.Order
// @Failure      401  {object}  map[string]string
// @Failure      404  {object}  map[string]string
// @Router       /api/user/orders/{uid} [get]
func (h *Handler) GetUserOrderByUIDHandle(c *gin.Context) {
	userID, ok := c.Get("user_id")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User is not logged",
		})
		return
	}
	userLogger := h.logger.With("user_id", userID)

	username, ok := c.Get("username")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User is not logged",
		})
		return
	}

	orderUID := c.Param("uid")
	if orderUID == "" {
		userLogger.Warn("no uid received")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No UID received",
		})
		return
	}

	if cachedOrder, exists := h.cache.Get(orderUID); exists {
		if cachedOrder.CustomerID == username.(string) {
			c.JSON(http.StatusOK, cachedOrder)
		} else {
			userLogger.Error("cached order doesn't belong to user")
			c.JSON(http.StatusNotFound, gin.H{
				"error": "Cached order doesn't belong to user",
			})
		}
		return
	}

	order, err := h.repo.GetOrderByUIDAndUser(c.Request.Context(), orderUID, username.(string))
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
// @Summary      Get all order UIDs (admin)
// @Description  Returns all order UIDs for admin users.
// @Tags         orders
// @Security     BearerAuth
// @Produce      json
// @Success      200  {object}  map[string][]string
// @Failure      401  {object}  map[string]string
// @Router       /api/admin/all_orders_uids [get]
func (h *Handler) GetAllOrdersUIDHandle(c *gin.Context) {
	userID, ok := c.Get("user_id")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User is not logged",
		})
		return
	}
	userLogger := h.logger.With("user_id", userID)

	orderUIDs, err := h.repo.GetAllOrdersUID(c.Request.Context())
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

// GetAllOrdersUIDHandle returns all users order UIDs as JSON.
// @Summary      Get own order UIDs (user)
// @Description  Returns order UIDs belonging to the authenticated user.
// @Tags         orders
// @Security     BearerAuth
// @Produce      json
// @Success      200  {object}  map[string][]string
// @Failure      401  {object}  map[string]string
// @Router       /api/user/all_orders_uids [get]
func (h *Handler) GetAllUserOrdersUIDHandle(c *gin.Context) {
	userID, ok := c.Get("user_id")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User is not logged",
		})
		return
	}
	userLogger := h.logger.With("user_id", userID)
	username, _ := c.Get("username")

	orderUIDs, err := h.repo.GetAllOrdersUIDByUser(c.Request.Context(), username.(string))
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
	userID, ok := c.Get("user_id")
	if !ok {
		h.logger.Error("user is not logged")
		c.HTML(http.StatusUnauthorized, "error.html", gin.H{
			"error": "User is not logged",
		})
		return
	}
	username, _ := c.Get("username")
	userLogger := h.logger.With("user_id", userID)

	role, ok := c.Get("role")
	if !ok {
		userLogger.Error("user has no role")
		c.HTML(http.StatusUnauthorized, "error.html", gin.H{
			"error": "User has no role",
		})
		return
	}

	var orderUIDs []string
	var err error
	if role.(string) == "admin" {
		orderUIDs, err = h.repo.GetAllOrdersUID(c.Request.Context())
	} else {
		orderUIDs, err = h.repo.GetAllOrdersUIDByUser(c.Request.Context(), username.(string))
	}

	if err != nil {
		userLogger.Error("failed to load orders", "error", err)
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
	userID, ok := c.Get("user_id")
	if !ok {
		h.logger.Error("user is not logged")
		c.HTML(http.StatusUnauthorized, "error.html", gin.H{
			"error": "User is not logged",
		})
		return
	}
	userLogger := h.logger.With("user_id", userID)
	username, _ := c.Get("username")

	role, ok := c.Get("role")
	if !ok {
		userLogger.Error("user has no role")
		c.HTML(http.StatusUnauthorized, "error.html", gin.H{
			"error": "User has no role",
		})
		return
	}

	orderUID := c.Param("uid")

	if orderUID == "" {
		userLogger.Error("no uid received")
		c.HTML(http.StatusBadRequest, "error.html", gin.H{
			"error": "No UID received",
		})
		return
	}

	if cachedOrder, exists := h.cache.Get(orderUID); exists {
		if cachedOrder.CustomerID == username.(string) {
			c.HTML(http.StatusOK, "order.html", cachedOrder)
		} else {
			userLogger.Error("cached order doesn't belong to user")
			c.HTML(http.StatusNotFound, "error.html", gin.H{
				"error": "Cached order doesn't belong to user",
			})
		}
		return
	}

	var order *models.Order
	var err error
	if role.(string) == "admin" {
		order, err = h.repo.GetOrderByUID(c.Request.Context(), orderUID)
	} else {
		order, err = h.repo.GetOrderByUIDAndUser(c.Request.Context(), orderUID, username.(string))
	}

	if err != nil {
		h.logger.Error("failed to get info by uid and user", "error", err)
		if errors.Is(err, pgx.ErrNoRows) {
			userLogger.Error("orders not found")
			c.HTML(http.StatusNotFound, "error.html", gin.H{
				"error": "Orders not found",
			})
		} else {
			userLogger.Error("internal server error")
			c.HTML(http.StatusInternalServerError, "error.html", gin.H{
				"error": "Internal server error",
			})
		}
		return
	}

	h.cache.Set(order)
	c.HTML(http.StatusOK, "order.html", order)
}

// VersionHandle returns the application version and commit hash.
//
//	@Summary		Get version info
//	@Description	Returns the build version and commit hash of the service.
//	@Tags			system
//	@Produce		json
//	@Success		200	{object}	map[string]string
//	@Router			/api/version [get]
func (h *Handler) VersionHandle(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"version": h.version,
		"commit":  h.commit,
	})
}
