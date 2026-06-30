package main

import (
	"context"
    "math/rand/v2"
	"encoding/json"
    "os"
	"fmt"
	"log"
	"time"
    "path/filepath"

	"github.com/go-playground/validator/v10"
	"github.com/segmentio/kafka-go"
    "github.com/google/uuid"

    "github.com/venexene/gorder/internal/models"
)

func loadOrderFromFile(path string) (*models.Order, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("Failed to read file %s: %v", path, err)
    }

    var order models.Order
    if err := json.Unmarshal(data, &order); err != nil {
        return nil, fmt.Errorf("Failed to unmarshal JSON: %v", err)
    }

    validate := validator.New()
    if err := validate.Struct(order); err != nil {
        return nil, fmt.Errorf("Failed to validate order: %v", err)
    }

    return &order, nil
}


func main() {
	kafkaBrokers := []string{"kafka:9092"}
	topic := "wbl0_orders"

    // Создание райтера Kafka
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers...), // Преобразование адреса брокера в TCP-формат
		Topic:    topic, // Установка топика
	}
	defer writer.Close() // Отложенное закрытие соединения с райтером
    
    // Отправка 10 сообщений о добавлении случайных заказов в БД
    for i := 0; i < 10; i++ {
        order := generateRandomOrder()

        // Отправка сообщения в Kafka
        message, err := json.Marshal(order)
        if err != nil {
            log.Printf("Failed to marshal order: %v", err)
			continue
        }

        // Отправка сообщения в Kafka
        err = writer.WriteMessages(context.Background(),
            kafka.Message{
                Key:   []byte(order.OrderUID), // Ключа сообщения
                Value: message, // Тело сообщения
            },
        )

        if err != nil {
            log.Fatalf("Failed to write message: %v", err)
        } else {
            log.Printf("Message successfully sent to Kafka")
        }

        time.Sleep(1 * time.Second)
    }

    
    // Отправка заказов, загружаемых из файлов
    for i := 1; i < 6; i++ {
        filename := filepath.Join("testdata", fmt.Sprintf("order%d.json", i))
        order, err := loadOrderFromFile(filename)
        if err != nil {
             log.Printf("Failed to marshal order: %v", err)
             continue
        }

        // Отправка сообщения в Kafka
        message, err := json.Marshal(order)
        if err != nil {
            log.Printf("Failed to marshal order: %v", err)
			continue
        }

        // Отправка сообщения в Kafka
        err = writer.WriteMessages(context.Background(),
            kafka.Message{
                Key:   []byte(order.OrderUID), // Ключа сообщения
                Value: message, // Тело сообщения
            },
        )

        if err != nil {
            log.Fatalf("Failed to write message: %v", err)
        } else {
            log.Printf("Message successfully sent to Kafka")
        }

        time.Sleep(1 * time.Second)
    }
}

// Генерация случайного заказа
func generateRandomOrder() models.Order {
    orderUID := uuid.New().String()
    currentTime := time.Now()

    delivery := models.Delivery {
        OrderUID: orderUID,
        Name:    generateRandomString(100),
        Phone:   generateRandomPhone(),
        Zip:     fmt.Sprintf("%d", rand.IntN(100000)),
        City:    generateRandomString(20),
        Address: generateRandomString(20),
        Region:  generateRandomString(20),
        Email:   generateRandomEmail(),
    }

    payment := models.Payment {
        OrderUID:     orderUID,
        Transaction:  orderUID,
        RequestID:    generateRandomString(10),
        Currency:     "USD",
        Provider:     "WBPAY",
        Amount:       rand.IntN(100000) + 10,
        PaymentDt:    uint64(currentTime.Unix()) - rand.Uint64N(1000000),
        Bank:         generateRandomString(15),
        DeliveryCost: rand.UintN(1000),
        GoodsTotal:   rand.UintN(500),
        CustomFee:    rand.UintN(100),
    }

    itemsCount := rand.IntN(5) + 1 // Случайное число предметов
    var items []models.Item
    for i := 0; i < itemsCount; i++ {
        item := models.Item {
            OrderUID:    orderUID,
            ChrtID:      rand.UintN(100000) + 1,
            TrackNumber: generateRandomString(10), 
            Price:       rand.UintN(100000),
            Rid:         generateRandomString(20),
            Name:        generateRandomString(15),
            Sale:        rand.UintN(99),
            Size:        fmt.Sprintf("%d", rand.UintN(5) + 1),
            TotalPrice:  rand.UintN(100000),
            NmID:        rand.UintN(100000) + 1,
            Brand:       generateRandomString(12),
            Status:      rand.UintN(999),
        }
        items = append(items, item)
    }

    order := models.Order {
        OrderUID:          orderUID,
        TrackNumber:       generateRandomString(10),
        Entry:             generateRandomString(4),
        Locale:            "ru",
        InternalSignature: generateRandomString(10),
        CustomerID:        generateRandomString(8),
        DeliveryService:   generateRandomString(7),
        ShardKey:          fmt.Sprintf("%d", rand.IntN(10)),
        SMID:              rand.UintN(100) + 1,
        DateCreated:       currentTime,
        OOFShard:          fmt.Sprintf("%d", rand.IntN(10)),
        Delivery:          delivery,
        Payment:           payment,
        Items:             items,
    }
    
    return order
}


// Генерация случайной строки
func generateRandomString(length int) string {
    const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    b := make([]byte, length)
    for i := range b {
        b[i] = charset[rand.IntN(len(charset))]
    }
    return string(b)
}


// Генерация слайного номера телефона
func generateRandomPhone() string {
    return fmt.Sprintf("+7%d", rand.IntN(1000000000))
}

// Генерация случайного email
func generateRandomEmail() string {
    return fmt.Sprintf("%s@%s.com", generateRandomString(10), generateRandomString(5))
}