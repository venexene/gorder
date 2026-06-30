FROM golang:1.25

# Установка рабочей директории
WORKDIR /app

# Копирование файлов зависимостей
COPY go.mod go.sum ./
RUN go mod download

# Копирование проекта
COPY . .

# Сборка приложения
RUN go build -o emulator ./cmd/emulator

# Запуск эмулятора-продюсера
CMD ["./emulator"]