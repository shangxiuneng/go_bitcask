package index

import (
	"errors"
	"github.com/google/btree"
	"github.com/rs/zerolog/log"
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

func (b *BTree) Iterator(reverse bool) Iterator {
	return NewBTreeIterator(b.tree, reverse)
}

// BTreeIterator Btree的索引迭代器
type BTreeIterator struct {
	Iterator

	currIndex int
	reverse   bool    // 是否为反向遍历
	values    []*Item // key位置对应的value
}

func NewBTreeIterator(tree *btree.BTree, reverse bool) Iterator {
	idx := 0

	values := make([]*Item, tree.Len())

	iterator := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		// 反向迭代
		tree.Descend(iterator)
	} else {
		tree.Ascend(iterator)
	}

	return &BTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (b *BTreeIterator) Rewind() {

}

func (b *BTreeIterator) Seek(key []byte) {

}

func (b *BTreeIterator) Next() {
	b.currIndex++
}

// Valid 当前迭代器是否有效
func (b *BTreeIterator) Valid() bool {
	return b.currIndex < len(b.values)
}

// Key 当前迭代器指向的key数据
func (b *BTreeIterator) Key() []byte {
	if b.currIndex >= len(b.values) {
		log.Error().Msgf("currIdx = %v,len(b.values) = %v", b.currIndex, len(b.values))
		return nil
	}
	return b.values[b.currIndex].key
}

func (b *BTreeIterator) Value() *data.RecordPos {
	return b.values[b.currIndex].record
}

// Close 关闭迭代器 释放相应的资源
func (b *BTreeIterator) Close() {
	b.values = nil
}
