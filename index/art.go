package index

import (
	"errors"
	art "github.com/plar/go-adaptive-radix-tree"
	"go_bitcask/data"
	"sync"
)

// ArtTree 自适应基数树
type ArtTree struct {
	Index

	lock    *sync.Mutex
	artTree art.Tree
}

func newArtTree() Index {
	return &ArtTree{
		artTree: art.New(),
		lock:    new(sync.Mutex),
	}
}

func (a *ArtTree) Put(key []byte, record *data.RecordPos) error {
	a.artTree.Insert(key, record)
	return nil
}
func (a *ArtTree) Get(key []byte) (*data.RecordPos, error) {
	if len(key) == 0 {
		return nil, errors.New("key is nil")
	}
	value, found := a.artTree.Search(key)
	if !found {
		return nil, nil
	}

	return value.(*data.RecordPos), nil
}

func (a *ArtTree) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("key is nil")
	}
	a.lock.Lock()
	defer a.lock.Unlock()

	a.artTree.Delete(key)
	return nil
}

func (a *ArtTree) Iterator(reverse bool) Iterator {
	return newArtIterator(a.artTree, reverse)
}

// art树的迭代器
type artIterator struct {
	Iterator

	currIndex int
	reverse   bool    // 是否为反向遍历
	values    []*Item // key位置对应的value
}

func newArtIterator(tree art.Tree, reverse bool) Iterator {

	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node art.Node) bool {
		item := &Item{
			key:    node.Key(),
			record: node.Value().(*data.RecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}

	return &artIterator{
		reverse: reverse,
	}
}

// Rewind 重新回到迭代器的起点
func (a *artIterator) Rewind() {
	a.currIndex = 0
}

// Seek 根据传入的key，查找到第一个大于或小于目标的key 从这个key开始遍历
func (a *artIterator) Seek(key []byte) {
	// TODO
	panic("Seek")
}

func (a *artIterator) Next() {
	a.currIndex++
}

// Valid 当前迭代器是否有效
func (a *artIterator) Valid() bool {
	return a.currIndex < len(a.values)
}

// Key 当前迭代器指向的key数据
func (a *artIterator) Key() []byte {
	return a.values[a.currIndex].key
}

func (a *artIterator) Value() *data.RecordPos {
	return a.values[a.currIndex].record
}

// Close 关闭迭代器 释放相应的资源
func (a *artIterator) Close() {
	a.currIndex = len(a.values) + 1
}
