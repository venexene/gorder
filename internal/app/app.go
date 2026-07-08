package app

import (
	"context"
	"fmt"
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

type Dependencies struct {
	Storage  *storage.Storage
    Consumer *consumer.Consumer
    Cache    *cache.Cache
    Metrics  *metrics.Metrics
    Config   *config.Config
    Logger   *slog.Logger
}

func Run() error {
	dep := &Dependencies{}
	var err error

	dep.Config, err = config.Load(".env")
	if err != nil {
		slog.Error("failed to load config", "error", err)
		return fmt.Errorf("Failed to load config")
	}
	slog.Info("loaded config")

	var logHandler slog.Handler
	if dep.Config.LogFormat == "json" {
		logHandler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		logHandler = slog.NewTextHandler(os.Stdout, nil)
	}
	dep.Logger = slog.New(logHandler)
	dep.Logger.Info("created logger")

	dep.Metrics = metrics.NewMetrics()
	dep.Logger.Info("created metrics")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := storage.CreatePool(ctx, dep.Config)
	if err != nil {
		dep.Logger.Error("failed to connect database", "error", err)
		return fmt.Errorf("Failed to connect database")
	}
	defer pool.Close()

	dep.Storage = storage.NewStorage(pool, dep.Config.MigrationDir)
	dep.Logger.Info("connected database")

	if err := dep.Storage.RunMigrations(); err != nil {
		dep.Logger.Error("failed to migrate database", "error", err)
		return fmt.Errorf("Failed to migrate database")
	}

	dep.Cache = cache.NewCache(dep.Config.CacheCapacity, dep.Logger, dep.Metrics)
	dep.Logger.Info("created cache")

	if err := dep.Cache.Populate(ctx, dep.Storage); err != nil {
		dep.Logger.Error("failed to populate cache", "error", err)
	} else {
		dep.Logger.Info("populated cache with orders", "count", dep.Cache.Size())
	}

	reader := createKafkaReader(dep.Config)
	dep.Consumer = consumer.NewConsumer(reader, dep.Storage, dep.Cache, dep.Logger, dep.Metrics, dep.Config.KafkaBrokers)
	defer dep.Consumer.Close()
	dep.Logger.Info("created message consumer")

	go func() {
		dep.Consumer.Consume(ctx)
	}()
	dep.Logger.Info("started consume process", "topic", dep.Config.KafkaTopic)

	router, err := createRouter(dep)
	if err != nil {
		dep.Logger.Error("failed to create router", "error", err)
		return fmt.Errorf("Failed to create router")
	}
	dep.Logger.Info("created router")

	srv := &http.Server{
		Addr:         ":" + dep.Config.HTTPPort,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	dep.Logger.Info("created server")

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	dep.Logger.Info("started HTTP server on port", "port", dep.Config.HTTPPort)

	select {
	case <-ctx.Done():
		dep.Logger.Info("shutting down server...")

		ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctxShutdown); err != nil {
			dep.Logger.Error("failed to shutdown server", "error", err)
			return fmt.Errorf("Failed to shutdown server")
		}
		dep.Logger.Info("shutdown server")
	case err := <-errCh:
		dep.Logger.Error("HTTP server error", "error", err)
		return fmt.Errorf("HTTP server error")
	}
	return nil
}

func createKafkaReader(cfg *config.Config) *kafka.Reader {
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
	return reader
}

func createRouter(dep *Dependencies) (*gin.Engine, error) {
	router := gin.Default()

	router.SetHTMLTemplate(template.Must(
		template.ParseFS(gorder.TemplatesFS, "web/templates/*"),
	))
	sub, err := fs.Sub(gorder.StaticFS, "web/static")
	if err != nil {
		dep.Logger.Error("failed to init static filesystem", "error", err)
		return nil, fmt.Errorf("Failed to init static filesystem")
	}
	router.StaticFS("/static", http.FS(sub))

	handler := handlers.NewHandler(dep.Storage, dep.Consumer, dep.Cache, dep.Logger)

	router.Use(middleware.MetricsMiddleware(dep.Metrics))
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
	protected.Use(middleware.JWTAuth(dep.Config.JWTSecret))
	{
		protected.GET("/orders/:uid", func(c *gin.Context) {
			handler.GetOrderByUIDHandle(c)
		})

		protected.GET("/all_orders_uids", func(c *gin.Context) {
			handler.GetAllOrdersUIDHandle(c)
		})
	}

	return router, nil
}