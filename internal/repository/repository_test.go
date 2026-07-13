package repository

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/venexene/gorder/internal/models"
)

var (
	testStorage *Repository
	testPool    *pgxpool.Pool
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres",
		postgres.WithDatabase("test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
	)

	if err != nil {
		log.Printf("failed to run container with postgres: %v", err)
		os.Exit(1)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("failed to run connection string from container: %v", err)
		os.Exit(1)
	}

	testPool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		log.Printf("failed to create pool: %v", err)
		os.Exit(1)
	}

	testStorage = NewStorage(testPool, "../../migrations")

	time.Sleep(2 * time.Second)

	if err := testStorage.RunMigrations(); err != nil {
		log.Printf("failed to migrate database: %v", err)
		os.Exit(1)
	}

	code := m.Run()

	testPool.Close()
	pgContainer.Terminate(ctx)

	os.Exit(code)
}

func makeTestOrder(uid string) models.Order {
	return models.Order{
		OrderUID:        uid,
		TrackNumber:     "TRACK123",
		Entry:           "TEST",
		Locale:          "en",
		CustomerID:      "cust1",
		DeliveryService: "dhl",
		ShardKey:        "1",
		SMID:            1,
		DateCreated:     time.Now(),
		OOFShard:        "1",
		Delivery: models.Delivery{
			Name:    "John Doe",
			Phone:   "+79991234567",
			Zip:     "12345",
			City:    "TestCity",
			Address: "123 Test St",
			Region:  "TestRegion",
			Email:   "test@example.com",
		},
		Payment: models.Payment{
			Transaction:  "b1e8a5d2-3c4f-4a1b-9d6e-7f8c0a1b2c3d",
			Currency:     "USD",
			Provider:     "stripe",
			Amount:       1000,
			PaymentDt:    1700000000,
			Bank:         "testbank",
			DeliveryCost: 200,
			GoodsTotal:   800,
			CustomFee:    0,
		},
		Items: []models.Item{
			{
				ChrtID:      1,
				TrackNumber: "TRACK123",
				Price:       800,
				Rid:         "rid1",
				Name:        "Test Item",
				Sale:        0,
				Size:        "M",
				TotalPrice:  800,
				NmID:        1,
				Brand:       "TestBrand",
				Status:      202,
			},
		},
	}
}

func setupTest(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	testPool.Exec(ctx, "DELETE FROM item")
	testPool.Exec(ctx, "DELETE FROM payment")
	testPool.Exec(ctx, "DELETE FROM delivery")
	testPool.Exec(ctx, "DELETE FROM orders")
}

func TestAddOrder_InsertAndRetrieve(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	uid := "e1a2b3c4-d5e6-7f8a-9b0c-1d2e3f4a5b6c"
	order := makeTestOrder(uid)

	if err := testStorage.AddOrder(ctx, &order); err != nil {
		t.Fatalf("AddOrder failed: %v", err)
	}

	fetched, err := testStorage.GetOrderByUID(ctx, uid)
	if err != nil {
		t.Fatalf("GetOrderByUID failed: %v", err)
	}

	if fetched.OrderUID != uid {
		t.Errorf("OrderUID: want %s, got %s", uid, fetched.OrderUID)
	}
	if fetched.TrackNumber != "TRACK123" {
		t.Errorf("TrackNumber mismatch: got %s", fetched.TrackNumber)
	}
	if fetched.Entry != "TEST" {
		t.Errorf("Entry mismatch: got %s", fetched.Entry)
	}
	if fetched.Locale != "en" {
		t.Errorf("Locale mismatch: got %s", fetched.Locale)
	}
	if fetched.CustomerID != "cust1" {
		t.Errorf("CustomerID mismatch: got %s", fetched.CustomerID)
	}
	if fetched.DeliveryService != "dhl" {
		t.Errorf("DeliveryService mismatch: got %s", fetched.DeliveryService)
	}
	if fetched.ShardKey != "1" {
		t.Errorf("ShardKey mismatch: got %s", fetched.ShardKey)
	}
	if fetched.SMID != 1 {
		t.Errorf("SMID mismatch: got %d", fetched.SMID)
	}
	if fetched.OOFShard != "1" {
		t.Errorf("OOFShard mismatch: got %s", fetched.OOFShard)
	}

	if fetched.Delivery.Name != "John Doe" {
		t.Errorf("Delivery.Name mismatch: got %s", fetched.Delivery.Name)
	}
	if fetched.Delivery.Phone != "+79991234567" {
		t.Errorf("Delivery.Phone mismatch: got %s", fetched.Delivery.Phone)
	}
	if fetched.Delivery.Zip != "12345" {
		t.Errorf("Delivery.Zip mismatch: got %s", fetched.Delivery.Zip)
	}
	if fetched.Delivery.City != "TestCity" {
		t.Errorf("Delivery.City mismatch: got %s", fetched.Delivery.City)
	}
	if fetched.Delivery.Address != "123 Test St" {
		t.Errorf("Delivery.Address mismatch: got %s", fetched.Delivery.Address)
	}
	if fetched.Delivery.Region != "TestRegion" {
		t.Errorf("Delivery.Region mismatch: got %s", fetched.Delivery.Region)
	}
	if fetched.Delivery.Email != "test@example.com" {
		t.Errorf("Delivery.Email mismatch: got %s", fetched.Delivery.Email)
	}

	if fetched.Payment.Transaction != "b1e8a5d2-3c4f-4a1b-9d6e-7f8c0a1b2c3d" {
		t.Errorf("Payment.Transaction mismatch: got %s", fetched.Payment.Transaction)
	}
	if fetched.Payment.Currency != "USD" {
		t.Errorf("Payment.Currency mismatch: got %s", fetched.Payment.Currency)
	}
	if fetched.Payment.Provider != "stripe" {
		t.Errorf("Payment.Provider mismatch: got %s", fetched.Payment.Provider)
	}
	if fetched.Payment.Amount != 1000 {
		t.Errorf("Payment.Amount mismatch: got %d", fetched.Payment.Amount)
	}
	if fetched.Payment.PaymentDt != 1700000000 {
		t.Errorf("Payment.PaymentDt mismatch: got %d", fetched.Payment.PaymentDt)
	}
	if fetched.Payment.Bank != "testbank" {
		t.Errorf("Payment.Bank mismatch: got %s", fetched.Payment.Bank)
	}
	if fetched.Payment.DeliveryCost != 200 {
		t.Errorf("Payment.DeliveryCost mismatch: got %d", fetched.Payment.DeliveryCost)
	}
	if fetched.Payment.GoodsTotal != 800 {
		t.Errorf("Payment.GoodsTotal mismatch: got %d", fetched.Payment.GoodsTotal)
	}
	if fetched.Payment.CustomFee != 0 {
		t.Errorf("Payment.CustomFee mismatch: got %d", fetched.Payment.CustomFee)
	}

	if len(fetched.Items) != 1 {
		t.Fatalf("Items count: want 1, got %d", len(fetched.Items))
	}
	item := fetched.Items[0]
	if item.ChrtID != 1 {
		t.Errorf("Item.ChrtID mismatch: got %d", item.ChrtID)
	}
	if item.TrackNumber != "TRACK123" {
		t.Errorf("Item.TrackNumber mismatch: got %s", item.TrackNumber)
	}
	if item.Price != 800 {
		t.Errorf("Item.Price mismatch: got %d", item.Price)
	}
	if item.Rid != "rid1" {
		t.Errorf("Item.Rid mismatch: got %s", item.Rid)
	}
	if item.Name != "Test Item" {
		t.Errorf("Item.Name mismatch: got %s", item.Name)
	}
	if item.Sale != 0 {
		t.Errorf("Item.Sale mismatch: got %d", item.Sale)
	}
	if item.Size != "M" {
		t.Errorf("Item.Size mismatch: got %s", item.Size)
	}
	if item.TotalPrice != 800 {
		t.Errorf("Item.TotalPrice mismatch: got %d", item.TotalPrice)
	}
	if item.NmID != 1 {
		t.Errorf("Item.NmID mismatch: got %d", item.NmID)
	}
	if item.Brand != "TestBrand" {
		t.Errorf("Item.Brand mismatch: got %s", item.Brand)
	}
	if item.Status != 202 {
		t.Errorf("Item.Status mismatch: got %d", item.Status)
	}
}

func TestAddOrderIfNotExists_Duplicate(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	uid := "f2b3c4d5-e6f7-8a9b-0c1d-2e3f4a5b6c7d"
	order := makeTestOrder(uid)

	if err := testStorage.AddOrderIfNotExists(ctx, &order); err != nil {
		t.Fatalf("first AddOrderIfNotExists failed: %v", err)
	}

	err := testStorage.AddOrderIfNotExists(ctx, &order)
	if err == nil {
		t.Fatal("expected error on duplicate, got nil")
	}
}

func TestGetOrderByUID_NotFound(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	_, err := testStorage.GetOrderByUID(ctx, "00000000-0000-0000-0000-000000000000")
	if err == nil {
		t.Fatal("expected error for non-existent UID, got nil")
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Errorf("expected pgx.ErrNoRows, got: %v", err)
	}
}

func TestOrderExists(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	uid := "a3b4c5d6-e7f8-9a0b-1c2d-3e4f5a6b7c8d"
	order := makeTestOrder(uid)
	if err := testStorage.AddOrder(ctx, &order); err != nil {
		t.Fatalf("AddOrder failed: %v", err)
	}

	exists, err := testStorage.OrderExists(ctx, uid)
	if err != nil {
		t.Fatalf("OrderExists failed: %v", err)
	}
	if !exists {
		t.Error("expected order to exist")
	}

	exists, err = testStorage.OrderExists(ctx, "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("OrderExists failed: %v", err)
	}
	if exists {
		t.Error("expected order to not exist")
	}
}

func TestGetAllOrdersUID(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	uids := []string{
		"c4d5e6f7-a8b9-4a0b-1c2d-3e4f5a6b7c8d",
		"d5e6f7a8-b9c0-4a1b-2c3d-4e5f6a7b8c9d",
		"e6f7a8b9-c0d1-4a2b-3c4d-5e6f7a8b9c0d",
	}
	for _, uid := range uids {
		order := makeTestOrder(uid)
		if err := testStorage.AddOrder(ctx, &order); err != nil {
			t.Fatalf("AddOrder failed for %s: %v", uid, err)
		}
	}

	all, err := testStorage.GetAllOrdersUID(ctx)
	if err != nil {
		t.Fatalf("GetAllOrdersUID failed: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("want 3 UIDs, got %d", len(all))
	}
}

func TestGetRecentOrdersUID(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	old := makeTestOrder("f7a8b9c0-d1e2-4a3b-4c5d-6e7f8a9b0c1d")
	old.DateCreated = time.Now().Add(-2 * time.Hour)

	recent := makeTestOrder("a8b9c0d1-e2f3-4a4b-5c6d-7e8f9a0b1c2d")
	recent.DateCreated = time.Now()

	if err := testStorage.AddOrder(ctx, &old); err != nil {
		t.Fatalf("AddOrder old failed: %v", err)
	}
	if err := testStorage.AddOrder(ctx, &recent); err != nil {
		t.Fatalf("AddOrder recent failed: %v", err)
	}

	uids, err := testStorage.GetRecentOrdersUID(ctx, 1)
	if err != nil {
		t.Fatalf("GetRecentOrdersUID failed: %v", err)
	}
	if len(uids) != 1 {
		t.Fatalf("want 1 UID, got %d", len(uids))
	}
	if uids[0] != recent.OrderUID {
		t.Errorf("expected most recent UID %s, got %s", recent.OrderUID, uids[0])
	}
}

func TestAddOrder_MultipleItems(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	uid := "b9c0d1e2-f3a4-4a5b-6c7d-8e9f0a1b2c3d"
	order := makeTestOrder(uid)
	order.Items = []models.Item{
		{ChrtID: 10, TrackNumber: "T1", Price: 100, Rid: "r1", Name: "Item A", Sale: 0, Size: "S", TotalPrice: 100, NmID: 10, Brand: "B1", Status: 200},
		{ChrtID: 20, TrackNumber: "T2", Price: 200, Rid: "r2", Name: "Item B", Sale: 5, Size: "L", TotalPrice: 190, NmID: 20, Brand: "B2", Status: 201},
		{ChrtID: 30, TrackNumber: "T3", Price: 300, Rid: "r3", Name: "Item C", Sale: 0, Size: "M", TotalPrice: 300, NmID: 30, Brand: "B3", Status: 202},
	}

	if err := testStorage.AddOrder(ctx, &order); err != nil {
		t.Fatalf("AddOrder failed: %v", err)
	}

	fetched, err := testStorage.GetOrderByUID(ctx, uid)
	if err != nil {
		t.Fatalf("GetOrderByUID failed: %v", err)
	}

	if len(fetched.Items) != 3 {
		t.Fatalf("want 3 items, got %d", len(fetched.Items))
	}
	if fetched.Items[0].Name != "Item A" {
		t.Errorf("item 0 name: got %s", fetched.Items[0].Name)
	}
	if fetched.Items[1].Name != "Item B" {
		t.Errorf("item 1 name: got %s", fetched.Items[1].Name)
	}
	if fetched.Items[2].Name != "Item C" {
		t.Errorf("item 2 name: got %s", fetched.Items[2].Name)
	}
}

func makeTestUser(username string) *models.User {
	return &models.User{
		Username:     username,
		PasswordHash: "$2a$10$test_hash_for_testing_purposes",
		Role:         "user",
	}
}

func TestCreateUser_Success(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	user := makeTestUser("alice_test")

	if err := testStorage.CreateUser(ctx, user); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	fetched, err := testStorage.GetUser(ctx, "alice_test")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	if fetched.Username != "alice_test" {
		t.Errorf("expected username 'alice', got %s", fetched.Username)
	}
	if fetched.PasswordHash != user.PasswordHash {
		t.Errorf("expected password_hash to match")
	}
	if fetched.Role != "user" {
		t.Errorf("expected role 'user', got %s", fetched.Role)
	}
	if fetched.ID == 0 {
		t.Error("expected non-zero ID after insert")
	}
}

func TestCreateUser_Duplicate(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	user := makeTestUser("bob_test")

	if err := testStorage.CreateUser(ctx, user); err != nil {
		t.Fatalf("first CreateUser failed: %v", err)
	}

	err := testStorage.CreateUser(ctx, user)
	if err == nil {
		t.Fatal("expected error on duplicate username, got nil")
	}
}

func TestGetUser_NotFound(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	_, err := testStorage.GetUser(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent user, got nil")
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Errorf("expected pgx.ErrNoRows, got: %v", err)
	}
}

func TestGetUser_Found(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	user := makeTestUser("charlie_test")
	if err := testStorage.CreateUser(ctx, user); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	fetched, err := testStorage.GetUser(ctx, "charlie_test")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	if fetched.Username != "charlie_test" {
		t.Errorf("expected username 'charlie', got %s", fetched.Username)
	}
	if fetched.Role != "user" {
		t.Errorf("expected role 'user', got %s", fetched.Role)
	}
}
