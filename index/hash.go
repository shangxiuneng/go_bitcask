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

func (h *Hash) Put(key []byte, record *data.RecordPos) error {
	if len(key) == 0 {
		return errors.New("key is nil")
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	h.m[string(key)] = record
	return nil
}
func (h *Hash) Get(key []byte) (*data.RecordPos, error) {
	h.lock.RLock()
	h.lock.RUnlock()
	if value, ok := h.m[string(key)]; ok {
		return value, nil
	}
	return nil, errors.New("key is not exist")
}
func (h *Hash) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("key is nil")
	}

	h.lock.Lock()
	h.lock.Unlock()
	delete(h.m, string(key))

	return nil
}

func (h *Hash) Iterator(reverse bool) Iterator {
	return nil
}
