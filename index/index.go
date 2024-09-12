package index

import (
	"go_bitcask/data"
)

type Index interface {
	Put(key []byte, record *data.RecordPos) error
	Get(key []byte) (*data.RecordPos, error)
	Delete(key []byte) error
}

// IndexType 索引类型
type IndexType int

const (
	// BTreeIndex btree索引
	BTreeIndex IndexType = 1
	// HashIndex 哈希索引
	HashIndex IndexType = 2
)

// NewIndex 可能有多种内存索引
func NewIndex(indexType IndexType) Index {
	switch indexType {
	case BTreeIndex:
		return newBTree(32)
	}

	return nil
}
