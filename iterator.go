package go_bitcask

import (
	"github.com/rs/zerolog/log"
	"go_bitcask/data"
	"go_bitcask/index"
)

// Iterator 面向用户的迭代器
type Iterator struct {
	indexIt index.Iterator
	db      *DB
	options IteratorConfig // 迭代器配置
}

func (d *DB) NewIterator(config IteratorConfig) *Iterator {
	it := d.index.Iterator(config.Reverse)

	return &Iterator{
		indexIt: it,
		db:      d,
		options: config,
	}
}

// Rewind 重新回到迭代器的起点
func (i *Iterator) Rewind() {
	i.indexIt.Rewind()
}

// Seek 根据传入的key，查找到第一个大于或小于目标的key 从这个key开始遍历
func (i *Iterator) Seek(key []byte) {
	i.indexIt.Seek(key)
}

func (i *Iterator) Next() {
	i.indexIt.Next()
}

// Valid 当前迭代器是否有效
func (i *Iterator) Valid() bool {
	return i.indexIt.Valid()
}

// Key 当前迭代器指向的key数据
func (i *Iterator) Key() []byte {
	return i.indexIt.Key()
}

func (i *Iterator) Value() *data.RecordInfo {
	// 通过位置索引信息获取value
	pos := i.indexIt.Value()
	recordInfo, err := i.db.getValueByPos(pos)
	if err != nil {
		log.Error().Msgf("getValueByPos error,err = %v", err)
		return nil
	}

	return recordInfo
}

// Close 关闭迭代器 释放相应的资源
func (i *Iterator) Close() {
	i.indexIt.Close()
}

// TODO
func skipToNext() {

}
