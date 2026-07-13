// Package repository provides database access using pgx and golang-migrate.
package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/venexene/gorder/internal/config"
	"github.com/venexene/gorder/internal/models"
)

// Repository provides database operations backed by a pgx connection pool.
type Repository struct {
	pool          *pgxpool.Pool
	migrationPath string
}

// Interface defines the contract for order storage operations.
type Interface interface {
	CheckHealthDB(ctx context.Context) error
	GetOrderByUID(ctx context.Context, orderUID string) (*models.Order, error)
	GetOrderByUIDAndUser(ctx context.Context, orderUID string, userID string) (*models.Order, error)
	GetAllOrdersUID(ctx context.Context) ([]string, error)
	GetAllOrdersUIDByUser(ctx context.Context, userID string) ([]string, error)
	GetRecentOrdersUID(ctx context.Context, limit int) ([]string, error)
	OrderExists(ctx context.Context, orderUID string) (bool, error)
	AddOrder(ctx context.Context, order *models.Order) error
	AddOrderIfNotExists(ctx context.Context, order *models.Order) error
	CreateUser(ctx context.Context, user *models.User) error
	GetUser(ctx context.Context, username string) (*models.User, error)
}

// NewStorage creates a new Repository with the given connection pool and migration path.
func NewStorage(pool *pgxpool.Pool, migrationPath string) *Repository {
	return &Repository{
		pool:          pool,
		migrationPath: migrationPath,
	}
}

// CreatePool opens a PostgreSQL connection pool using the provided config.
func CreatePool(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, error) {
	connectionStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		cfg.DBUser,
		cfg.DBPass,
		cfg.DBHost,
		cfg.DBPort,
		cfg.DBName,
		cfg.DBSSLMode,
	)

	poolCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(poolCtx, connectionStr)
	if err != nil {
		return pool, fmt.Errorf("failed to create pool: %v", err)
	}

	if err := pool.Ping(poolCtx); err != nil {
		return pool, fmt.Errorf("failed to ping database: %v", err)
	}

	return pool, nil
}

// RunMigrations applies all pending database migrations.
func (s *Repository) RunMigrations() error {
	connStr := s.pool.Config().ConnConfig.ConnString()

	m, err := migrate.New(fmt.Sprintf("file://%s", s.migrationPath), connStr)
	if err != nil {
		return fmt.Errorf("failed to init migrate: %v", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to up migrate: %v", err)
	}

	return nil
}

// CheckHealthDB verifies the database connection pool is alive by pinging the server.
func (s *Repository) CheckHealthDB(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// GetOrderByUID retrieves a complete order with delivery, payment and items.
func (s *Repository) GetOrderByUID(ctx context.Context, orderUID string) (*models.Order, error) {
	orderQuery := `
		SELECT
			o.order_uid, o.track_number, o.entry, o.locale,
			o.internal_signature, o.customer_id, o.delivery_service,
			o.shardkey, o.sm_id, o.date_created, o.oof_shard,
			d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction, p.request_id, p.currency, p.provider,
			p.amount, p.payment_dt, p.bank, p.delivery_cost,
			p.goods_total, p.custom_fee
		FROM orders o
		LEFT JOIN delivery d ON d.order_uid = o.order_uid
		LEFT JOIN payment p ON p.order_uid = o.order_uid
		WHERE o.order_uid = $1
	`
	var order models.Order
	err := s.pool.QueryRow(ctx, orderQuery, orderUID).Scan(
		&order.OrderUID,
		&order.TrackNumber,
		&order.Entry,
		&order.Locale,
		&order.InternalSignature,
		&order.CustomerID,
		&order.DeliveryService,
		&order.ShardKey,
		&order.SMID,
		&order.DateCreated,
		&order.OOFShard,
		&order.Delivery.Name,
		&order.Delivery.Phone,
		&order.Delivery.Zip,
		&order.Delivery.City,
		&order.Delivery.Address,
		&order.Delivery.Region,
		&order.Delivery.Email,
		&order.Payment.Transaction,
		&order.Payment.RequestID,
		&order.Payment.Currency,
		&order.Payment.Provider,
		&order.Payment.Amount,
		&order.Payment.PaymentDt,
		&order.Payment.Bank,
		&order.Payment.DeliveryCost,
		&order.Payment.GoodsTotal,
		&order.Payment.CustomFee,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("failed to find order with UID %v: %w", orderUID, err)
		}
		return nil, fmt.Errorf("failed to query database: %w", err)
	}

	itemsQuery := "SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status FROM item WHERE order_uid = $1"
	rows, err := s.pool.Query(ctx, itemsQuery, orderUID)
	if err != nil {
		return nil, fmt.Errorf("failed to query items: %w", err)
	}
	defer rows.Close()
	var items []models.Item
	for rows.Next() {
		var item models.Item
		err = rows.Scan(
			&item.ChrtID,
			&item.TrackNumber,
			&item.Price,
			&item.Rid,
			&item.Name,
			&item.Sale,
			&item.Size,
			&item.TotalPrice,
			&item.NmID,
			&item.Brand,
			&item.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan item: %w", err)
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate items: %w", err)
	}
	order.Items = items

	return &order, nil
}

// GetOrderByUIDAndUser retrieves a complete order with delivery, payment and items by order and user id.
func (s *Repository) GetOrderByUIDAndUser(ctx context.Context, orderUID string, userID string) (*models.Order, error) {
	orderQuery := `
		SELECT
			o.order_uid, o.track_number, o.entry, o.locale,
			o.internal_signature, o.customer_id, o.delivery_service,
			o.shardkey, o.sm_id, o.date_created, o.oof_shard,
			d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction, p.request_id, p.currency, p.provider,
			p.amount, p.payment_dt, p.bank, p.delivery_cost,
			p.goods_total, p.custom_fee
		FROM orders o
		LEFT JOIN delivery d ON d.order_uid = o.order_uid
		LEFT JOIN payment p ON p.order_uid = o.order_uid
		WHERE o.order_uid = $1 AND o.customer_id = $2
	`
	var order models.Order
	err := s.pool.QueryRow(ctx, orderQuery, orderUID, userID).Scan(
		&order.OrderUID,
		&order.TrackNumber,
		&order.Entry,
		&order.Locale,
		&order.InternalSignature,
		&order.CustomerID,
		&order.DeliveryService,
		&order.ShardKey,
		&order.SMID,
		&order.DateCreated,
		&order.OOFShard,
		&order.Delivery.Name,
		&order.Delivery.Phone,
		&order.Delivery.Zip,
		&order.Delivery.City,
		&order.Delivery.Address,
		&order.Delivery.Region,
		&order.Delivery.Email,
		&order.Payment.Transaction,
		&order.Payment.RequestID,
		&order.Payment.Currency,
		&order.Payment.Provider,
		&order.Payment.Amount,
		&order.Payment.PaymentDt,
		&order.Payment.Bank,
		&order.Payment.DeliveryCost,
		&order.Payment.GoodsTotal,
		&order.Payment.CustomFee,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("failed to find order with UID %v and User ID %v: %w", orderUID, userID, err)
		}
		return nil, fmt.Errorf("failed to query database: %w", err)
	}

	itemsQuery := "SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status FROM item WHERE order_uid = $1"
	rows, err := s.pool.Query(ctx, itemsQuery, orderUID)
	if err != nil {
		return nil, fmt.Errorf("failed to query items: %w", err)
	}
	defer rows.Close()
	var items []models.Item
	for rows.Next() {
		var item models.Item
		err = rows.Scan(
			&item.ChrtID,
			&item.TrackNumber,
			&item.Price,
			&item.Rid,
			&item.Name,
			&item.Sale,
			&item.Size,
			&item.TotalPrice,
			&item.NmID,
			&item.Brand,
			&item.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan item: %w", err)
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate items: %w", err)
	}
	order.Items = items

	return &order, nil
}

// AddOrder inserts an order and all related data in a single transaction.
func (s *Repository) AddOrder(ctx context.Context, order *models.Order) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				slog.Error("rollback failed", "error", rbErr)
			}
		}
	}()

	orderQuery := `
        INSERT INTO orders (
            order_uid, track_number, entry, locale, internal_signature,
            customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `
	_, err = tx.Exec(ctx, orderQuery,
		order.OrderUID,
		order.TrackNumber,
		order.Entry,
		order.Locale,
		order.InternalSignature,
		order.CustomerID,
		order.DeliveryService,
		order.ShardKey,
		order.SMID,
		order.DateCreated,
		order.OOFShard,
	)
	if err != nil {
		return fmt.Errorf("failed to insert order: %w", err)
	}

	deliveryQuery := `
        INSERT INTO delivery (
            order_uid, name, phone, zip, city, address, region, email
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `
	_, err = tx.Exec(ctx, deliveryQuery,
		order.OrderUID,
		order.Delivery.Name,
		order.Delivery.Phone,
		order.Delivery.Zip,
		order.Delivery.City,
		order.Delivery.Address,
		order.Delivery.Region,
		order.Delivery.Email,
	)
	if err != nil {
		return fmt.Errorf("failed to insert delivery: %w", err)
	}

	paymentQuery := `
        INSERT INTO payment (
            order_uid, transaction, request_id, currency, provider, amount, 
            payment_dt, bank, delivery_cost, goods_total, custom_fee
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `
	_, err = tx.Exec(ctx, paymentQuery,
		order.OrderUID,
		order.Payment.Transaction,
		order.Payment.RequestID,
		order.Payment.Currency,
		order.Payment.Provider,
		order.Payment.Amount,
		order.Payment.PaymentDt,
		order.Payment.Bank,
		order.Payment.DeliveryCost,
		order.Payment.GoodsTotal,
		order.Payment.CustomFee,
	)
	if err != nil {
		return fmt.Errorf("failed to insert payment: %w", err)
	}

	itemQuery := `
        INSERT INTO item (
            order_uid, chrt_id, track_number, price, rid, name, 
            sale, size, total_price, nm_id, brand, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    `
	for _, item := range order.Items {
		_, err = tx.Exec(ctx, itemQuery,
			order.OrderUID,
			item.ChrtID,
			item.TrackNumber,
			item.Price,
			item.Rid,
			item.Name,
			item.Sale,
			item.Size,
			item.TotalPrice,
			item.NmID,
			item.Brand,
			item.Status,
		)
		if err != nil {
			return fmt.Errorf("failed to insert item: %w", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// OrderExists checks whether an order with the given UID exists.
func (s *Repository) OrderExists(ctx context.Context, orderUID string) (bool, error) {
	query := "SELECT EXISTS(SELECT 1 FROM orders WHERE order_uid = $1)"
	var exists bool

	err := s.pool.QueryRow(ctx, query, orderUID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check order existence: %w", err)
	}

	return exists, nil
}

// AddOrderIfNotExists inserts an order only if its UID is not already present.
func (s *Repository) AddOrderIfNotExists(ctx context.Context, order *models.Order) error {
	exists, err := s.OrderExists(ctx, order.OrderUID)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("order with UID %v already exists", order.OrderUID)
	}

	return s.AddOrder(ctx, order)
}

// GetAllOrdersUID returns UIDs of all orders in the database.
func (s *Repository) GetAllOrdersUID(ctx context.Context) ([]string, error) {
	query := "SELECT order_uid FROM orders"
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query orders: %w", err)
	}
	defer rows.Close()

	var listUIDs []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, fmt.Errorf("failed to scan order_uid: %w", err)
		}
		listUIDs = append(listUIDs, uid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate order_uid: %w", err)
	}

	return listUIDs, nil
}

// GetAllOrdersUIDByUser returns UIDs of all orders of user in the database.
func (s *Repository) GetAllOrdersUIDByUser(ctx context.Context, userID string) ([]string, error) {
	query := "SELECT order_uid FROM orders WHERE customer_id = $1"
	rows, err := s.pool.Query(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to query orders: %w", err)
	}
	defer rows.Close()

	var listUIDs []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, fmt.Errorf("failed to scan order_uid: %w", err)
		}
		listUIDs = append(listUIDs, uid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate order_uid: %w", err)
	}

	return listUIDs, nil
}

// GetRecentOrdersUID returns UIDs of the most recent orders, limited by count.
func (s *Repository) GetRecentOrdersUID(ctx context.Context, limit int) ([]string, error) {
	query := "SELECT order_uid FROM orders ORDER BY date_created DESC LIMIT $1"

	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent orders: %w", err)
	}
	defer rows.Close()

	var uids []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, fmt.Errorf("failed to scan order_uid: %w", err)
		}
		uids = append(uids, uid)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate order_uid: %w", err)
	}

	return uids, nil
}

// CreateUser inserts a new user into the database.
func (s *Repository) CreateUser(ctx context.Context, user *models.User) error {
	query := `
        INSERT INTO users (
            username, password_hash, role
        ) VALUES ($1, $2, $3)
    `
	_, err := s.pool.Exec(ctx, query,
		user.Username,
		user.PasswordHash,
		user.Role,
	)

	if err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}

	return nil
}

// GetUser retrieves a user by username from the database.
func (s *Repository) GetUser(ctx context.Context, username string) (*models.User, error) {
	query := `
		SELECT id, username, password_hash, role FROM users WHERE username = $1
	`
	var user models.User
	err := s.pool.QueryRow(ctx, query, username).Scan(
		&user.ID,
		&user.Username,
		&user.PasswordHash,
		&user.Role,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("failed to find user with username %v: %w", username, err)
		}
		return nil, fmt.Errorf("failed to query database: %w", err)
	}

	return &user, nil
}
