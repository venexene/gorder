FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /app/main ./cmd


FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata curl
WORKDIR /app
COPY --from=builder /app/main .
COPY migrations/ ./migrations/
EXPOSE 8080
USER 10001:10001
CMD ["./main"]