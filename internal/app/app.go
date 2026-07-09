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
	"github.com/venexene/gorder/internal/handler"
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

	consumerDone := make(chan struct{})
	go func() {
		dep.Consumer.Consume(ctx)
		close(consumerDone)
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

		<-consumerDone

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

	hd := &handler.HandlerDependencies{
		Storage:  dep.Storage,
		Consumer: dep.Consumer,
		Cache:    dep.Cache,
		Logger:   dep.Logger,
		Config:   dep.Config,
	}
	handler := handler.NewHandler(hd)

	router.Use(middleware.MetricsMiddleware(dep.Metrics))
	router.Use(ginprom.Middleware())

	public := router.Group("")
	{
		public.GET("/health/live", func(c *gin.Context) {
			handler.LiveCheckHandle(c)
		})

		public.GET("/health/ready", func(c *gin.Context) {
			handler.ReadyCheckHandle(c)
		})

		public.GET("/metrics", gin.WrapH(ginprom.GetMetricHandler()))

		public.POST("/login", func(c *gin.Context) {
			handler.LoginHandle(c)
		})

		public.POST("/register", func(c *gin.Context) {
			handler.RegisterHandle(c)
		})

		public.POST("/refresh", func(c *gin.Context) {
			handler.RefreshHandle(c)
		})
	}

	user := router.Group("")
	user.Use(middleware.JWTAuth(dep.Config.JWTSecret))
	user.Use(middleware.RequireRole("user", "admin"))
	{
		user.GET("/", func(c *gin.Context) {
			handler.AllOrdersPageHandle(c)
		})

		user.GET("/:uid", func(c *gin.Context) {
			handler.OrderPageHandle(c)
		})
	}

	admin := router.Group("/api")
	admin.Use(middleware.JWTAuth(dep.Config.JWTSecret))
	admin.Use(middleware.RequireRole("admin"))
	{
		admin.GET("/orders/:uid", func(c *gin.Context) {
			handler.GetOrderByUIDHandle(c)
		})

		admin.GET("/all_orders_uids", func(c *gin.Context) {
			handler.GetAllOrdersUIDHandle(c)
		})

	}

	return router, nil
}
