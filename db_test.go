package go_bitcask

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_Put(t *testing.T) {
	db, err := Open(Config{
		DirPath: "temp",
	})
	assert.Nil(t, err)

	// put一条数据
	err = db.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)

	// 重复put一条数据
	err = db.Put([]byte("key"), []byte("value1"))
	assert.Nil(t, err)

	// key为空
	err = db.Put(nil, []byte("value1"))
	assert.Equal(t, ErrKeyIsNil, err)
	// value为空
	err = db.Put([]byte("key"), nil)
	assert.Nil(t, err)
	// 写到文件末尾  // TODO
	// 重启db后再次put  // TODO
}

func TestDB_Get(t *testing.T) {
	db, err := Open(Config{
		DirPath:      "temp",
		DataFileSize: 1000,
	})
	assert.Nil(t, err)

	// put一条数据
	err = db.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)

	value, err := db.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value"), value)
}
