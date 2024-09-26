package go_bitcask

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOpen(t *testing.T) {
	DefaultConfig.DirPath = "temp"
	db, err := Open(DefaultConfig)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

/*
case
1. put一条key为空的数据
2. put一条value为空的数据
3. put一条正常的数据
4. 重复put一条数据
5. 写到数据文件进行转换
6. 重启后put数据
*/
func TestDB_Put(t *testing.T) {
	config := DefaultConfig
	config.DirPath = "temp"
	config.DataFileSize = 1000

	db, err := Open(config)
	assert.Nil(t, err)

	// key为空
	err = db.Put(nil, []byte("value1"))
	assert.Equal(t, ErrKeyIsNil, err)
	// value为空
	err = db.Put([]byte("key"), nil)
	assert.Nil(t, err)

	// put一条数据
	err = db.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)

	// 重复put一条数据
	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)
	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	// 写到文件末尾
	for i := 0; i < 20; i++ {
		err = db.Put([]byte("key1"), []byte("value1"))
		assert.Nil(t, err)
	}

}
func TestDB_Put2(t *testing.T) {
	config := DefaultConfig
	config.DirPath = "temp"
	config.DataFileSize = 160

	db, err := Open(config)
	assert.Nil(t, err)

	// 写到文件末尾  // TODO 需要判断当前文件的个数
	for i := 0; i < 22; i++ {
		err = db.Put([]byte("1"), []byte("1"))
		assert.Nil(t, err)
	}
}
func TestDB_Put3(t *testing.T) {
	config := DefaultConfig
	config.DirPath = "temp"
	config.DataFileSize = 1000

	db, err := Open(config)
	assert.Nil(t, err)

	// put一条数据
	err = db.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)

	// 关闭db
	err = db.Close()
	assert.Nil(t, err)

	// 重新打开db
	db2, err := Open(config)
	assert.Nil(t, err)
	// 重复put一条数据
	err = db2.Put([]byte("key2"), []byte("value1"))
	assert.Nil(t, err)
	err = db2.Put([]byte("key3"), []byte("value1"))
	assert.Nil(t, err)
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
