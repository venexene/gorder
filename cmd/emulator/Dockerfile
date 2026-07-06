FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o emulator ./cmd/emulator

FROM alpine:3.24
WORKDIR /app
COPY --from=builder /app/emulator .
CMD ["./emulator"]