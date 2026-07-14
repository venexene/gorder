FROM golang:1.25 AS builder
ARG VERSION=dev
ARG COMMIT=unknown
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
RUN go install github.com/swaggo/swag/cmd/swag@latest
COPY . .
RUN swag init -g cmd/main.go
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" -o /app/main ./cmd


FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata curl
WORKDIR /app
COPY --from=builder /app/main .
COPY migrations/ ./migrations/
EXPOSE 8080
USER 10001:10001
CMD ["./main"]