package index

import (
	"errors"
	"github.com/google/btree"
	"go_bitcask/data"
	"sync"
)

type BTree struct {
	Index

	tree *btree.BTree
	lock *sync.Mutex
}

func newBTree(degree int) *BTree {
	return &BTree{
		tree: btree.New(degree), // degree btree的度
		lock: &sync.Mutex{},
	}
}

// Put 写入元素
func (b *BTree) Put(key []byte, record *data.RecordPos) error {
	if key == nil {
		return errors.New("key is nil")
	}

	b.lock.Lock()
	defer b.lock.Unlock()
	b.tree.ReplaceOrInsert(&Item{
		key:    key,
		record: record,
	})

	return nil
}

// Get 获取一条数据
func (b *BTree) Get(key []byte) (*data.RecordPos, error) {
	item := b.tree.Get(&Item{
		key: key,
	})
	return item.(*Item).record, nil
}

// Delete 删除一条记录
func (b *BTree) Delete(key []byte) error {
	if key == nil {
		return errors.New("key is nil")
	}
	b.lock.Lock()
	defer b.lock.Unlock()

	b.tree.Delete(&Item{
		key: key,
	})

	return nil
}
