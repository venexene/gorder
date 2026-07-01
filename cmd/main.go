package main

import (
	"context"
	"log"
	"net/http"
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
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Println("Loaded config")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := database.CreatePool(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	defer pool.Close()

	storage := database.NewStorage(pool, cfg.MigrationDir)
	log.Println("Connected database")

	if err := storage.RunMigrations(); err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	cache := cache.NewCache(cfg.CacheCapacity)
	log.Println("Created cache")

	if err := cache.Populate(ctx, storage); err != nil {
		log.Printf("Failed to populate cache: %v", err)
	} else {
		log.Printf("Populated cache with %d orders", cache.Size())
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
	)
	defer messageConsumer.Close()
	log.Println("Created message consumer")

	go func() {
		messageConsumer.Consume(ctx)
	}()
	log.Printf("Started consume proccess for topic %s", cfg.KafkaTopic)

	router := gin.Default()
	log.Printf("Created GIN router")

	router.LoadHTMLGlob("web/templates/*")
	router.Static("/static", "./web/static")

	handler := handlers.NewHandler(storage, cache, cfg.KafkaBrokers)

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
	log.Printf("Created server")

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
	log.Printf("Started HTTP server on port %s", cfg.HTTPPort)

	<-ctx.Done()
	stop()
	log.Println("Shutting down server...")

	ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctxShutdown); err != nil {
		log.Fatalf("Failed to shutdown server: %v", err)
	}
	log.Println("Shutdown server")
}
