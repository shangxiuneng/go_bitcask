package index

import (
	"errors"
	"go_bitcask/data"
	"sync"
)

type Hash struct {
	Index

	lock *sync.RWMutex
	m    map[string]*data.RecordPos
}

func newHashIndex() Index {
	return &Hash{
		lock: new(sync.RWMutex),
		m:    make(map[string]*data.RecordPos),
	}
}

func (h *Hash) Put(key []byte, record *data.RecordPos) (*data.RecordPos, error) {
	if len(key) == 0 {
		return nil, errors.New("key is nil")
	}
	h.lock.Lock()
	defer h.lock.Unlock()

	oldRecord := h.m[string(key)]

	h.m[string(key)] = record
	return oldRecord, nil
}
func (h *Hash) Get(key []byte) (*data.RecordPos, error) {
	h.lock.RLock()
	h.lock.RUnlock()
	if value, ok := h.m[string(key)]; ok {
		return value, nil
	}
	return nil, errors.New("key is not exist")
}
func (h *Hash) Delete(key []byte) (*data.RecordPos, error) {
	if len(key) == 0 {
		return nil, errors.New("key is nil")
	}

	h.lock.Lock()
	h.lock.Unlock()

	oldRecord := h.m[string(key)]

	delete(h.m, string(key))

	return oldRecord, nil
}
func (h *Hash) Iterator(reverse bool) Iterator {
	return newHashIterator(h.m, reverse)
}

func (h *Hash) Close() error {
	return nil
}

type hashIterator struct {
	Iterator

	currIndex int
	reverse   bool    // 是否为反向遍历
	values    []*Item // key位置对应的value
}

func newHashIterator(hash map[string]*data.RecordPos, reverse bool) Iterator {

	idx := 0
	items := make([]*Item, len(hash))

	for key, value := range hash {
		items[idx] = &Item{
			key:    []byte(key),
			record: value,
		}
		idx++
	}

	// TODO 对items进行一个排序

	return &hashIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    items,
	}

}

// Rewind 重新回到迭代器的起点
func (h *hashIterator) Rewind() {
	h.currIndex = 0
	return
}

// Seek 根据传入的key，查找到第一个大于或小于目标的key 从这个key开始遍历
func (h *hashIterator) Seek(key []byte) {

	return
}

func (h *hashIterator) Next() {
	h.currIndex++
}

// Valid 当前迭代器是否有效
func (h *hashIterator) Valid() bool {
	return h.currIndex < len(h.values)
}

// Key 当前迭代器指向的key数据
func (h *hashIterator) Key() []byte {
	if !h.Valid() {
		return nil
	}
	return h.values[h.currIndex].key
}

func (h *hashIterator) Value() *data.RecordPos {
	if !h.Valid() {
		return nil
	}
	return h.values[h.currIndex].record
}

// Close 关闭迭代器 释放相应的资源
func (h *hashIterator) Close() {
	h.values = nil
	h.currIndex = len(h.values) + 1
}
