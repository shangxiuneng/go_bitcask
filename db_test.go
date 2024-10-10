package go_bitcask

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go_bitcask/index"
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

/*
测试用例
1. 正常操作 put后get
2. get一个不存在的key
3. key为nil
*/
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

	// get一个不存在的key
	value, err = db.Get([]byte("not_exist_key"))
	assert.Nil(t, err)
	assert.Nil(t, value)

	// key为nil
	value, err = db.Get(nil)
	assert.NotNil(t, err)
	assert.Nil(t, value)
}

/*
delete的测试用例
1. put数据后再删除 获取数据
2. 删除一个不存在的key
3. 删除为nil的key
4. 值被删除后再重新put
5. 写入值 删除 重启db 再次获取值
*/
func TestDB_Delete(t *testing.T) {
	db, err := Open(Config{
		DirPath:      "temp",
		DataFileSize: 1000,
	})
	assert.Nil(t, err)

	// put数据后删除
	err = db.Put([]byte("delete"), []byte("value"))
	assert.Nil(t, err)

	err = db.Delete([]byte("delete"))
	assert.Nil(t, err)

	value, err := db.Get([]byte("delete"))
	assert.Nil(t, err)
	assert.Nil(t, value)

	// 删除一个不存在的key
	err = db.Delete([]byte("not_exist_key"))
	assert.Nil(t, err)

	// key为nil
	err = db.Delete(nil)
	assert.NotNil(t, err)

	// 值被删除后重新put
	err = db.Put([]byte("delete1"), []byte("value"))
	assert.Nil(t, err)

	err = db.Delete([]byte("delete1"))
	assert.Nil(t, err)

	err = db.Put([]byte("delete1"), []byte("value1"))
	assert.Nil(t, err)

	value, err = db.Get([]byte("delete1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), value)

	// 写入值 删除 重启db 再次获取值
	err = db.Put([]byte("delete2"), []byte("value"))
	assert.Nil(t, err)

	err = db.Delete([]byte("delete2"))
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	db, err = Open(Config{
		DirPath:      "temp",
		DataFileSize: 1000,
	})
	assert.Nil(t, err)

	value, err = db.Get([]byte("delete2"))
	assert.Nil(t, err)
	assert.Nil(t, value)
}

/*
测试用例
1. db为空
2. db包含多条数据
*/
func TestDB_ListKeys(t *testing.T) {
	db, err := Open(Config{
		DirPath:      "temp",
		DataFileSize: 1000,
		// IndexType:    index.HashIndex,
	})
	assert.Nil(t, err)

	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))

	// put多条数据
	for i := 0; i < 10; i++ {
		tempKey := fmt.Sprintf("key%d", i)
		err = db.Put([]byte(tempKey), []byte("value"))
		assert.Nil(t, err)
	}

	// TODO listKeys是有序还是无序的
	keys = db.ListKeys()
	assert.Equal(t, 10, len(keys))

	for i, v := range keys {
		tempKey := fmt.Sprintf("key%d", i)
		assert.Equal(t, []byte(tempKey), v)
		t.Logf("key = %v", string(v))
	}
}

func TestDB_Fold(t *testing.T) {
	db, err := Open(Config{
		DirPath:      "temp",
		DataFileSize: 1000,
		IndexType:    index.HashIndex,
	})
	assert.Nil(t, err)

	// put多条数据
	for i := 0; i < 10; i++ {
		tempKey := fmt.Sprintf("key%d", i)
		err = db.Put([]byte(tempKey), []byte("value"))
		assert.Nil(t, err)
	}

	err = db.Fold(func(key []byte, value []byte) bool {
		t.Logf(string(key))
		t.Logf(string(value))
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)
}
