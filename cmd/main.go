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

	"github.com/venexene/gorder/internal/cache"
	"github.com/venexene/gorder/internal/config"
	"github.com/venexene/gorder/internal/database"
	"github.com/venexene/gorder/internal/handlers"
	consumer "github.com/venexene/gorder/internal/kafka"
)

func main() {
	// Получение конфигураций
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Println("Loaded config")

	// Создание контекста для получения сигнала о завершении
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Подключение к БД через создание пула соединений
	pool, err := database.CreatePool(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	defer pool.Close()
	storage := database.NewStorage(pool)
	log.Println("Connected database")

	// Создание кэша
	cache := cache.NewCache(cfg.CacheCapacity)
	log.Println("Created cache")

	// Заполнение кэша
	if err := cache.Populate(ctx, storage); err != nil {
		log.Printf("Failed to populate cache: %v", err)
	} else {
		log.Printf("Populated cache with %d orders", cache.Size())
	}

	// Создание консьюмера Kafka
	kafkaConsumer := consumer.NewConsumer(
		strings.Split(cfg.KafkaBrokers, ","),
		cfg.KafkaTopic,
		storage,
		cache,
	)
	defer kafkaConsumer.Close()
	log.Println("Created Kafka consumer")

	//Запуск консьюмера в горутине
	go func() {
		kafkaConsumer.Consume(ctx)
	}()
	log.Printf("Started consume proccess for topic %s", cfg.KafkaTopic)

	// Создание роутера
	router := gin.Default()
	log.Printf("Created GIN router")

	router.LoadHTMLGlob("web/templates/*")   // Загрузка HTML шаблонов
	router.Static("/static", "./web/static") // Загрузка статических файлов

	// Создание хендлера
	handler := handlers.NewHandler(storage, cfg, cache)

	//Тестовый эндпоинт для проверки работы сервера
	router.GET("/api/server_check", func(c *gin.Context) {
		handler.TestServerHandle(c)
	})

	//Тестовый эндпоинт для проверки подключения к базе
	router.GET("/api/db_check", func(c *gin.Context) {
		handler.TestDBHandle(c)
	})

	//Тестовый эндпоинт для проверки работы Kafka
	router.GET("/api/kafka_check", func(c *gin.Context) {
		handler.TestKafkaHandle(c)
	})

	//Эндпоинт для получения информации о заказе по UID
	router.GET("/api/orders/:uid", func(c *gin.Context) {
		handler.GetOrderByUIDHandle(c)
	})

	//Эндпоинт для получения UID всех заказов
	router.GET("/api/all_orders_uids", func(c *gin.Context) {
		handler.GetAllOrdersUIDHandle(c)
	})

	// Эндпоинт для основной страницы со списком заказов
	router.GET("/", func(c *gin.Context) {
		handler.AllOrdersPageHandle(c)
	})

	// Эндпоинт для страницы о заказе
	router.GET("/:uid", func(c *gin.Context) {
		handler.OrderPageHandle(c)
	})

	// Создание сервера
	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: router,
	}
	log.Printf("Created server")

	// Запуск сервера в горутине
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
	log.Printf("Started HTTP server on port %s", cfg.HTTPPort)

	// Ожидание сигнала завершения
	<-ctx.Done()
	stop() // Отмена подписки на сигнал
	log.Println("Shutting down server...")

	//Создание контекста с таймаутом для корректного завершения
	ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Закрытие сервера
	if err := srv.Shutdown(ctxShutdown); err != nil {
		log.Fatalf("Failed to shutdown server: %v", err)
	}
	log.Println("Shutdown server")
}
