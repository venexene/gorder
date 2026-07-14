// Package app wires dependencies, starts the HTTP server, and handles graceful shutdown.
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
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/ulule/limiter/v3"
	"github.com/ulule/limiter/v3/drivers/store/memory"

	"github.com/venexene/gorder"
	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/config"
	"github.com/venexene/gorder/internal/consumer"
	"github.com/venexene/gorder/internal/handler"
	"github.com/venexene/gorder/internal/metrics"
	"github.com/venexene/gorder/internal/middleware"
	"github.com/venexene/gorder/internal/repository"
)

type Dependencies struct {
	Repository      *repository.Repository
	Consumer        *consumer.Consumer
	Cache           *cache.Cache
	Metrics         *metrics.Metrics
	Config          *config.Config
	Logger          *slog.Logger
	Limiter         *limiter.Limiter
	RegisterLimiter *limiter.Limiter
	Version         string
	Commit          string
}

func Run(dep *Dependencies) error {
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
	pool, err := repository.CreatePool(ctx, dep.Config)
	if err != nil {
		dep.Logger.Error("failed to connect database", "error", err)
		return fmt.Errorf("Failed to connect database")
	}
	defer pool.Close()

	dep.Repository = repository.NewStorage(pool, dep.Config.MigrationDir)
	dep.Logger.Info("connected database")

	if err := dep.Repository.RunMigrations(); err != nil {
		dep.Logger.Error("failed to migrate database", "error", err)
		return fmt.Errorf("Failed to migrate database")
	}

	dep.Cache = cache.NewCache(dep.Config.CacheCapacity, dep.Logger, dep.Metrics)
	dep.Logger.Info("created cache")

	if err := dep.Cache.Populate(ctx, dep.Repository); err != nil {
		dep.Logger.Error("failed to populate cache", "error", err)
	} else {
		dep.Logger.Info("populated cache with orders", "count", dep.Cache.Size())
	}

	reader := createKafkaReader(dep.Config)
	dep.Consumer = consumer.NewConsumer(reader, dep.Repository, dep.Cache, dep.Logger, dep.Metrics, dep.Config.KafkaBrokers)
	defer dep.Consumer.Close()
	dep.Logger.Info("created message consumer")

	consumerDone := make(chan struct{})
	go func() {
		dep.Consumer.Consume(ctx)
		close(consumerDone)
	}()
	dep.Logger.Info("started consume process", "topic", dep.Config.KafkaTopic)

	dep.Limiter, err = createRateLimiter(dep.Config.RateLimit)
	if err != nil {
		dep.Logger.Error("failed to create rate limiter", "error", err)
		return fmt.Errorf("Failed to create rate limiter")
	}
	dep.Logger.Info("started rate limiter", "limit", dep.Config.RateLimit)

	dep.RegisterLimiter, err = createRateLimiter(dep.Config.RateLimitRegister)
	if err != nil {
		dep.Logger.Error("failed to create register rate limiter", "error", err)
		return fmt.Errorf("Failed to create register rate limiter")
	}
	dep.Logger.Info("started register rate limiter", "limit", dep.Config.RateLimitRegister)

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

func createRateLimiter(formatted string) (*limiter.Limiter, error) {
	rate, err := limiter.NewRateFromFormatted(formatted)
	if err != nil {
		return nil, err
	}

	store := memory.NewStore()

	limit := limiter.New(store, rate)

	return limit, nil
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
		Repository: dep.Repository,
		Consumer:   dep.Consumer,
		Cache:      dep.Cache,
		Logger:     dep.Logger,
		Config:     dep.Config,
	}
	handler := handler.NewHandler(hd)

	router.Use(middleware.MetricsMiddleware(dep.Metrics))
	router.Use(ginprom.Middleware())

	public := router.Group("")
	{
		public.GET("/health/live", handler.LiveCheckHandle)

		public.GET("/health/ready", handler.ReadyCheckHandle)

		public.GET("/metrics", gin.WrapH(ginprom.GetMetricHandler()))

		public.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

		public.GET("/api/version", handler.VersionHandle)

		public.GET("/login", handler.LoginPageHandle)

		public.POST("/login", middleware.RateLimitMiddleware(dep.Limiter), handler.LoginHandle)

		public.POST("/logout", handler.LogoutHandle)

		public.GET("/register", handler.RegisterPageHandle, middleware.RateLimitMiddleware(dep.Limiter))

		public.POST("/register", handler.RegisterHandle)

		public.POST("/refresh", middleware.RateLimitMiddleware(dep.Limiter), handler.RefreshHandle)
	}

	user := router.Group("")
	user.Use(middleware.JWTAuth(dep.Config.JWTSecret))
	user.Use(middleware.RequireRole("user", "admin"))
	{
		user.GET("/", handler.AllOrdersPageHandle)

		user.GET("/:uid", handler.OrderPageHandle)
	}

	adminAPI := router.Group("/api/admin")
	adminAPI.Use(middleware.JWTAuth(dep.Config.JWTSecret))
	adminAPI.Use(middleware.RequireRole("admin"))
	{
		adminAPI.GET("/orders/:uid", handler.GetOrderByUIDHandle)

		adminAPI.GET("/all_orders_uids", handler.GetAllOrdersUIDHandle)
	}

	userAPI := router.Group("/api/user")
	userAPI.Use(middleware.JWTAuth(dep.Config.JWTSecret))
	userAPI.Use(middleware.RequireRole("user"))
	{
		userAPI.GET("/orders/:uid", handler.GetUserOrderByUIDHandle)

		userAPI.GET("/all_orders_uids", handler.GetAllUserOrdersUIDHandle)
	}

	return router, nil
}
