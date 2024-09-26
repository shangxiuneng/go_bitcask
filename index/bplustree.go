package index

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"
	"go_bitcask/data"
	"path/filepath"
)

var indexBucketName = []byte("bitcask-index")

const bptreeIndexFileName = "bptree-index"

// BPlusTree B+树
type BPlusTree struct {
	Index

	bPlusTree *bbolt.DB
}

func newBPlusTree(dirPath string, syncWrites bool) Index {
	db, err := bbolt.Open(
		filepath.Join(dirPath, bptreeIndexFileName),
		0644,
		bbolt.DefaultOptions)
	if err != nil {
		log.Error().Msgf("open error,err = %v", err)
		return nil
	}

	// 创建对应的 bucket
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		log.Error().Msgf("open error,err = %v", err)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{
		bPlusTree: db,
	}
}

func (b *BPlusTree) Put(key []byte, record *data.RecordPos) error {
	if len(key) == 0 {
		return errors.New("key is nil")
	}

	err := b.bPlusTree.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(indexBucketName)
		if err != nil {
			return err
		}

		// 将 key-value 写入到 bucket 中
		err = bucket.Put(key, data.EncodeRecordPos(record))
		if err != nil {
			return err
		}
		return nil
	})

	return err
}
func (b *BPlusTree) Get(key []byte) (*data.RecordPos, error) {
	var dataRecordPos []byte
	// 创建一个 read-only 事务来获取数据
	err := b.bPlusTree.View(func(tx *bbolt.Tx) error {
		// 获取对应的 bucket
		bucket := tx.Bucket(indexBucketName)
		// 如果 bucket 返回为 nil，则说明不存在对应 bucket
		if bucket == nil {
			return errors.New("bucket is nil")
		}
		// 从 bucket 中获取对应的 key（即上面写入的 key-value）
		dataRecordPos = bucket.Get(key)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return data.DecodeRecordPos(dataRecordPos)
}
func (b *BPlusTree) Delete(key []byte) error {
	return nil
}
func (b *BPlusTree) Iterator(reverse bool) Iterator {
	return nil
}
