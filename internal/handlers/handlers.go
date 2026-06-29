package handlers

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
    
	"github.com/venexene/wbl0-orders-service/internal/cache"
	"github.com/venexene/wbl0-orders-service/internal/config"
	"github.com/venexene/wbl0-orders-service/internal/database"
)

// Структура хендлера
type Handler struct {
    storage database.StorageInterface
    cfg     *config.Config
    cache   *cache.Cache
}

// Конструктор структуры хендлера
func NewHandler(storage database.StorageInterface, cfg *config.Config, cache *cache.Cache) *Handler {
    return &Handler{
        storage: storage,
        cfg:     cfg,
        cache:   cache,
    }
}


// Хендлер для обработки тестового запроса к серверу
func (h *Handler) TestServerHandle(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{
        "status": "Server definetly works",
    })
}


// Хендлер для обработки тестового запроса к БД
func (h *Handler) TestDBHandle(c *gin.Context) {
    res, err := h.storage.TestDB(c.Request.Context())
    if err != nil {
        log.Printf("Failed to test database: %v", err)
        c.JSON(http.StatusInternalServerError, gin.H{
            "error": "Failed to connect database",
        })
        return
    }

    c.JSON(http.StatusOK, gin.H{
        "status": res,
    })
}


// Хендлер для обработки тестового запроса к Kafka
func (h *Handler) TestKafkaHandle(c *gin.Context) {
    kafkaBrokers := h.cfg.KafkaBrokers

    // Контекст с таймаутом для ограничения времени выполнения операции с Kafka
    ctxKafka, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
    defer cancel() // Отложенный вызов отмены контекса для освобождения ресурсов

    // Установка соединений с Kafka
    connKafka, err := kafka.DialContext(ctxKafka, "tcp", kafkaBrokers)
    if err != nil {
        log.Printf("Failed to test Kafka: %v", err)
        c.JSON(http.StatusInternalServerError, gin.H{
            "error" : "Failed to connect Kafka",
        })
        return
    }
    defer connKafka.Close() // Отложенное закрытие соединения с Kafka

    broker := connKafka.Broker() // Получение информации о брокере
    c.JSON(http.StatusOK, gin.H {
        "status":    "Kafka definetly works",
        "brokers":   kafkaBrokers,
        "broker_id": broker.ID,
    })
}


// Хендлер для обработки запроса на получение всей информации о заказе по UID
func (h *Handler) GetOrderByUIDHandle(c *gin.Context) {
    orderUID := c.Param("uid") // Извлечение UID из URL
    
    // Проверка что передан не пустой UID
    if orderUID == "" {
        c.JSON(http.StatusBadRequest, gin.H{
            "error": "No UID recieved",
        })
        return
    }

    //Проверка наличия в кэше
    if cachedOrder, exists := h.cache.Get(orderUID); exists {
        c.JSON(http.StatusOK, cachedOrder)
        return
    }

    // Вызов функции получения всей информации о заказе по UID
    order, err := h.storage.GetOrderByUID(c.Request.Context(), orderUID)
    
    //Обработка ошибок получения заказа
    if err != nil {
        log.Printf("Failed to get info by UID: %v", err)
        if errors.Is(err, pgx.ErrNoRows) {
            c.JSON(http.StatusNotFound, gin.H{
                "error": "Failed to find order",
            })
        } else {
            c.JSON(http.StatusInternalServerError, gin.H{
                "error": "Internal server error",
            })
        }
        return
    }

    h.cache.Set(order)
    c.JSON(http.StatusOK, order)
}


// Хендлер для получения UID всех заказов
func (h *Handler) GetAllOrdersUIDHandle(c *gin.Context) {
    orderUIDs, err := h.storage.GetAllOrdersUID(c.Request.Context())

    if err != nil {
        log.Printf("Failed to get UIDs: %v", err)
        c.JSON(http.StatusInternalServerError, gin.H{
            "error": "Failed to get order UIDs",
        })
        return
    }

    c.JSON(http.StatusOK, gin.H{
        "order_uids": orderUIDs,
    })
}


// Хендлер для страницы основной страницы со всеми заказами
func (h* Handler) AllOrdersPageHandle(c *gin.Context) {
    orderUIDs, err := h.storage.GetAllOrdersUID(c.Request.Context())

    if err != nil {
        log.Printf("Failed to get UIDs: %v", err)
        c.HTML(http.StatusInternalServerError, "error.html", gin.H{
            "error": "Failed to load orders",
        })
        return
    }

    c.HTML(http.StatusOK, "orders.html", gin.H{
        "orders": orderUIDs,
    })
}


// Хендлер для страницы о заказе
func (h* Handler) OrderPageHandle(c *gin.Context) {
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
        log.Printf("Failed to get info by UID: %v", err)
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