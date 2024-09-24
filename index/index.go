package index

import (
	"github.com/rs/zerolog/log"
	"go_bitcask/data"
)

type Index interface {
	Put(key []byte, record *data.RecordPos) error
	Get(key []byte) (*data.RecordPos, error)
	Delete(key []byte) error

	Iterator(reverse bool) Iterator
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
	case HashIndex:
		return newHashIndex()
	default:
		log.Error().Msgf("undefined index type = %v", indexType)
	}

	return nil
}

// Iterator 迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点
	Rewind()

	// Seek 根据传入的key，查找到第一个大于或小于目标的key 从这个key开始遍历
	Seek(key []byte)

	Next()

	// Valid 当前迭代器是否有效
	Valid() bool

	// Key 当前迭代器指向的key数据
	Key() []byte

	Value() *data.RecordPos

	// Close 关闭迭代器 释放相应的资源
	Close()
}
