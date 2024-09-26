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
	return nil
}

// art树的迭代器
type artIterator struct {
}