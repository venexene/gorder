package main

import (
	"context"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	ginprom "github.com/logocomune/gin-prometheus"
	"github.com/segmentio/kafka-go"

	"github.com/venexene/gorder"
	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/config"
	"github.com/venexene/gorder/internal/consumer"
	"github.com/venexene/gorder/internal/handlers"
	"github.com/venexene/gorder/internal/metrics"
	"github.com/venexene/gorder/internal/middleware"
	"github.com/venexene/gorder/internal/storage"
)

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
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

	m := metrics.NewMetrics()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := storage.CreatePool(ctx, cfg)
	if err != nil {
		logger.Error("failed to connect database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	s := storage.NewStorage(pool, cfg.MigrationDir)
	logger.Info("connected database")

	if err := s.RunMigrations(); err != nil {
		logger.Error("failed to migrate database", "error", err)
		os.Exit(1)
	}

	c := cache.NewCache(cfg.CacheCapacity, logger, m)
	logger.Info("created cache")

	if err := c.Populate(ctx, s); err != nil {
		logger.Error("failed to populate cache", "error", err)
	} else {
		logger.Info("populated cache with orders", "count", c.Size())
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
	co := consumer.NewConsumer(
		reader,
		s,
		c,
		logger,
		m,
		cfg.KafkaBrokers,
	)
	defer co.Close()
	logger.Info("created message consumer")

	go func() {
		co.Consume(ctx)
	}()
	logger.Info("started consume process", "topic", cfg.KafkaTopic)

	router := gin.Default()
	logger.Info("created GIN router")

	router.SetHTMLTemplate(template.Must(
		template.ParseFS(gorder.TemplatesFS, "web/templates/*"),
	))
	sub, err := fs.Sub(gorder.StaticFS, "web/static")
	if err != nil {
		logger.Error("failed to init static filesystem", "error", err)
		os.Exit(1)
	}
	router.StaticFS("/static", http.FS(sub))

	handler := handlers.NewHandler(s, co, c, logger)

	router.Use(middleware.MetricsMiddleware(m))
	router.Use(ginprom.Middleware())

	router.GET("/health/live", func(c *gin.Context) {
		handler.LiveCheckHandle(c)
	})

	router.GET("/health/ready", func(c *gin.Context) {
		handler.ReadyCheckHandle(c)
	})

	router.GET("/metrics", gin.WrapH(ginprom.GetMetricHandler()))

	public := router.Group("")
	{
		public.GET("/", func(c *gin.Context) {
			handler.AllOrdersPageHandle(c)
		})

		public.GET("/:uid", func(c *gin.Context) {
			handler.OrderPageHandle(c)
		})
	}

	protected := router.Group("/api")
	protected.Use(middleware.JWTAuth(cfg.JWTSecret))
	{
		protected.GET("/orders/:uid", func(c *gin.Context) {
			handler.GetOrderByUIDHandle(c)
		})

		protected.GET("/all_orders_uids", func(c *gin.Context) {
			handler.GetAllOrdersUIDHandle(c)
		})
	}

	srv := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
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
