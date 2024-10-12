package go_bitcask

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

// 无任何数据进行merge
func TestDB_Merge(t *testing.T) {
	db, err := Open(Config{
		DirPath:      "temp",
		DataFileSize: 1000,
	})
	assert.Nil(t, err)

	err = db.Merge()
	assert.Nil(t, err)
}

/*
全部都是有效数据
merge后
*/
func TestDB_Merge2(t *testing.T) {
	db, err := Open(Config{
		DirPath:      "temp",
		DataFileSize: 1000,
	})
	assert.Nil(t, err)
	if err != nil {
		db.Close()
		return
	}

	for i := 0; i < 1; i++ {
		tempKey := fmt.Sprintf("key%d", i)
		err = db.Put([]byte(tempKey), []byte("value"))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	//db2, err := Open(Config{
	//	DirPath:      "temp",
	//	DataFileSize: 1000,
	//})
	//assert.Nil(t, err)
	//assert.NotNil(t, db2)
	//
	//defer func() {
	//	db2.Close()
	//}()
	//
	//keys := db2.ListKeys()
	//
	//for _, key := range keys {
	//	value, err := db2.Get(key)
	//	assert.Nil(t, err)
	//	assert.Equal(t, []byte("value"), value)
	//}
}
