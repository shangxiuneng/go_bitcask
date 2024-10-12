package go_bitcask

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

/*
事务写入测试用例
1. 开启事务，写入数据，提交，查看是否能获取到相关的数据
2. 开启事务，但是不提交，查看内存索引中是否有相关的数据
3. 开启事务 写入数据 提交 重启数据库 重新获取事务写入的数据
4. 开启事务 写入数据，提交，重启数据库，再次开启事务，写入数据
*/
func TestWriteBatch_Put(t *testing.T) {
	db, err := Open(Config{
		DirPath:      "temp",
		DataFileSize: 1000,
	})
	assert.Nil(t, err)

	// 正常写入一条数据
	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	batch := db.NewWriteBatch(DefaultBatchConfig)
	// 写入一条数据
	err = batch.Put([]byte("key1"), []byte("value2"))
	assert.Nil(t, err)

	// 不提交
}

/*
事务删除测试用例
*/
func TestWriteBatch_Delete(t *testing.T) {

}

func TestWriteBatch_EncodeKeyWithSeqNo(t *testing.T) {
	encodeData := encodeKeyWithSeqNo([]byte("key1"), 1)
	decodeKeyWithSeqNo(encodeData)
}
