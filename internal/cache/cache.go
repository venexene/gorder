package cache

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/venexene/gorder/internal/database"
	"github.com/venexene/gorder/internal/models"
)

type cacheNode struct {
	key   string
	value *models.Order
	prev  *cacheNode
	next  *cacheNode
}

// Структура кэша
type Cache struct {
	capacity int
	elems    map[string] *cacheNode
	head     *cacheNode
	tail     *cacheNode
	mu       sync.RWMutex
}

// Конструктор кэша
func NewCache(capacity int) *Cache {
	elems := make(map[string] *cacheNode)

	cache := Cache {
		capacity: capacity,
		elems: elems,
		head: &cacheNode{},
		tail: &cacheNode{},
	}
	
	cache.head.next = cache.tail
	cache.tail.prev = cache.head

	return &cache
}



// Добавление узла
func (c *Cache) addNode(n *cacheNode) {
	n.prev = c.head
	n.next = c.head.next
	c.head.next.prev = n
	c.head.next = n
}

// Удаление узла
func (c* Cache) removeNode(n *cacheNode) {
	prev := n.prev
	next := n.next
	prev.next = next
	next.prev = prev
}

// Перенос узла в начало списка
func (c *Cache) moveToHead(n *cacheNode) {
	c.removeNode(n)
	c.addNode(n)
}

// Удаление последнего узла списка
func (c*Cache) popTail() *cacheNode {
	res := c.tail.prev
	c.removeNode(res)
	return res
}



// Добавление данных в кэш
func (c *Cache) Set(order *models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if n, exist := c.elems[order.OrderUID]; exist {
		n.value = order
		c.moveToHead(n)
		return
	}

	n := &cacheNode{key: order.OrderUID, value: order}
	c.elems[order.OrderUID] = n
	c.addNode(n)

	if len(c.elems) > c.capacity {
		tail := c.popTail()
		delete(c.elems, tail.key)
	}
}

// Получение данных из кэша
func (c *Cache) Get(key string) (*models.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if n, exist := c.elems[key]; exist {
		c.moveToHead(n)
		return n.value, true
	}

	return nil, false
}

// Удаление из кэша
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.elems, key)
}

// Получение UID всех заказов в кэше
func (c *Cache) GetAllUIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	uids := make([]string, 0, len(c.elems))
	for uid := range c.elems {
		uids = append(uids, uid)
	}
	return uids
}

// Получение размера кэша
func (c *Cache) Size() int { 
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.elems)
}

// Заполнение кэша
func (c *Cache) Populate(ctx context.Context, storage *database.Storage) error {
	uids, err := storage.GetRecentOrdersUID(ctx, c.capacity)
	if err != nil {
		return fmt.Errorf("Failed to get recent orders: %v", err)
	}

	var loadCount int
	for _, uid := range uids {
		order, err := storage.GetOrderByUID(ctx, uid)
		if err != nil {
			log.Printf("Failed to load order %s into cache: %v", uid, err)
			continue
		}
		c.Set(order)
		loadCount++
	}

	return nil
}