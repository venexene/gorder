package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/config"
	"github.com/venexene/gorder/internal/consumer"
	"github.com/venexene/gorder/internal/database"
	"github.com/venexene/gorder/internal/handlers"
)

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	var logHandler slog.Handler
	if cfg.LogFormat == "json" {
		logHandler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		logHandler = slog.NewTextHandler(os.Stdout, nil)
	}
	logger := slog.New(logHandler)

	logger.Info("loaded config")
	logger.Info("created logger")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := database.CreatePool(ctx, cfg)
	if err != nil {
		logger.Error("failed to connect database", "error", err)
    	os.Exit(1)
	}
	defer pool.Close()

	storage := database.NewStorage(pool, cfg.MigrationDir)
	logger.Info("connected database")

	if err := storage.RunMigrations(); err != nil {
		logger.Error("failed to migrate database", "error", err)
    	os.Exit(1)
	}

	cache := cache.NewCache(cfg.CacheCapacity, logger)
	logger.Info("created cache")

	if err := cache.Populate(ctx, storage); err != nil {
		logger.Error("failed to populate cache", "error", err)
	} else {
		logger.Info("populated cache with orders", "count", cache.Size())
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(cfg.KafkaBrokers, ","),
		Topic:    cfg.KafkaTopic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
		MaxWait:  time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		MaxAttempts: 3,
	})
	messageConsumer := consumer.NewConsumer(
		reader,
		storage,
		cache,
		logger,
	)
	defer messageConsumer.Close()
	logger.Info("created message consumer")

	go func() {
		messageConsumer.Consume(ctx)
	}()
	logger.Info("started consume process", "topic", cfg.KafkaTopic)

	router := gin.Default()
	logger.Info("created GIN router")

	router.LoadHTMLGlob("web/templates/*")
	router.Static("/static", "./web/static")

	handler := handlers.NewHandler(storage, cache, logger, cfg.KafkaBrokers)

	router.GET("/api/server_check", func(c *gin.Context) {
		handler.TestServerHandle(c)
	})

	router.GET("/api/db_check", func(c *gin.Context) {
		handler.TestDBHandle(c)
	})

	router.GET("/api/kafka_check", func(c *gin.Context) {
		handler.TestKafkaHandle(c)
	})

	router.GET("/api/orders/:uid", func(c *gin.Context) {
		handler.GetOrderByUIDHandle(c)
	})

	router.GET("/api/all_orders_uids", func(c *gin.Context) {
		handler.GetAllOrdersUIDHandle(c)
	})

	router.GET("/", func(c *gin.Context) {
		handler.AllOrdersPageHandle(c)
	})

	router.GET("/:uid", func(c *gin.Context) {
		handler.OrderPageHandle(c)
	})

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: router,
	}
	logger.Info("created server")

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
    		os.Exit(1)
		}
	}()
	logger.Info("started HTTP server on port", "port", cfg.HTTPPort)

	<-ctx.Done()
	stop()
	logger.Info("shutting down server...")

	ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctxShutdown); err != nil {
		logger.Error("failed to shutdown server", "error", err)
    	os.Exit(1)
	}
	logger.Info("shutdown server")
}
