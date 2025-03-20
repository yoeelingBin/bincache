package lru

import "container/list"

// Cache is a LRU cache. It is not safe for concurrent access.
type Cache struct {
	maxBytes  int64                         // maxBytes is the maximum memory allowed
	nbytes    int64                         // nbytes is the current memory used
	ll        *list.List                    // ll is the double linked list
	cache     map[string]*list.Element      // cache is the map to store the key and value
	OnEvicted func(key string, value Value) //	OnEvicted is a callback function when an entry is deleted
}

// entry is the value of the linked list
type entry struct {
	key   string
	value Value
}

// Value is the interface to get the size of the value
type Value interface {
	Len() int
}

// New is the constructor of Cache
func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

// Get is used to get the value of the key
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele) // 双向链表队首队尾是相对的，这里其实是队尾
		kv := ele.Value.(*entry)
		return kv.value, true
	}
	return
}

// RemoveOldest is used to remove the oldest element
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back() // 双向链表队首队尾是相对的，这里其实是队首
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)                                // 删除map中的元素
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len()) // 更新当前内存使用
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// Add is used to add the key and value to the cache
func (c *Cache) Add(key string, value Value) {
	if ele, ok := c.cache[key]; ok { //	如果存在，则更新
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		kv.value = value
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
	} else { // 不存在则添加
		ele := c.ll.PushFront(&entry{key, value})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	// 如果超过了最大内存，则删除最老的元素
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

// Len is used to get the length of the cache
func (c *Cache) Len() int {
	return c.ll.Len()
}
