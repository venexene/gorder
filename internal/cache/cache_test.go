package cache

import (
	"testing"

	"github.com/venexene/gorder/internal/models"
)

// TestCacheSetGet verifies basic Set/Get operations and non-existent key lookup.
func TestCacheSetGet(t *testing.T) {
	cache := NewCache(2)
	order, err := models.LoadOrderFromFile("../../testdata/order1.json")
	if err != nil {
		t.Errorf("Failed to load order from file: %v", err)
	}

	cache.Set(order)
	if cached, exist := cache.Get("1864b7f1-c455-4300-bfdc-d339429c2099"); !exist || cached.OrderUID != "1864b7f1-c455-4300-bfdc-d339429c2099" {
		t.Error("Failed to get cached order")
	}

	if _, exist := cache.Get("nonexistent"); exist {
		t.Error("Found non-existent key in cache")
	}
}

// TestCacheEviction verifies that the least recently used item is evicted when capacity is exceeded.
func TestCacheEviction(t *testing.T) {
	cache := NewCache(2)
	order1, err := models.LoadOrderFromFile("../../testdata/order1.json")
	if err != nil {
		t.Errorf("Failed to load order1 from file: %v", err)
	}

	order2, err := models.LoadOrderFromFile("../../testdata/order2.json")
	if err != nil {
		t.Errorf("Failed to load order2 from file: %v", err)
	}

	order3, err := models.LoadOrderFromFile("../../testdata/order3.json")
	if err != nil {
		t.Errorf("Failed to load order3 from file: %v", err)
	}

	cache.Set(order1)
	cache.Set(order2)
	cache.Set(order3)

	if _, exist := cache.Get("1864b7f1-c455-4300-bfdc-d339429c2099"); exist {
		t.Error("Failed to evict order1 from cache")
	}

	if _, exist := cache.Get("1234b7f1-c455-4300-bfdc-d339429c2099"); !exist {
		t.Error("Failed to contain order2 after eviction")
	}

	if _, exist := cache.Get("4321b7f1-c455-4300-bfdc-d339429c2099"); !exist {
		t.Error("Failed to contain order3 after eviction")
	}
}

// TestCacheGetAllUIDs verifies that GetAllUIDs returns all keys currently in the cache.
func TestCacheGetAllUIDs(t *testing.T) {
	cache := NewCache(3)

	order1, err := models.LoadOrderFromFile("../../testdata/order1.json")
	if err != nil {
		t.Errorf("Failed to load order1 from file: %v", err)
	}

	order2, err := models.LoadOrderFromFile("../../testdata/order2.json")
	if err != nil {
		t.Errorf("Failed to load order2 from file: %v", err)
	}

	order3, err := models.LoadOrderFromFile("../../testdata/order3.json")
	if err != nil {
		t.Errorf("Failed to load order3 from file: %v", err)
	}

	cache.Set(order1)
	cache.Set(order2)
	cache.Set(order3)

	uids := cache.GetAllUIDs()
	if len(uids) != 3 {
		t.Errorf("Expected 3 UIDs, but got %d", len(uids))
	}
}

// TestCacheDelete verifies that an order is properly removed from the cache.
func TestCacheDelete(t *testing.T) {
	cache := NewCache(2)

	order, err := models.LoadOrderFromFile("../../testdata/order1.json")
	if err != nil {
		t.Errorf("Failed to load order1 from file: %v", err)
	}

	cache.Set(order)
	cache.Delete("1864b7f1-c455-4300-bfdc-d339429c2099")

	if _, exist := cache.Get(""); exist {
		t.Error("Failed to delete order from cache")
	}
}
