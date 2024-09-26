package index

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go_bitcask/data"
	"strconv"
	"sync"
	"testing"
)

func TestHash_Put(t *testing.T) {
	hashIndex := newHashIndex()
	// key为nil
	err := hashIndex.Put(nil, &data.RecordPos{FileID: 1, Offset: 10})
	assert.NotNil(t, err)
	t.Logf("err = %v", err)

	// key正常
	err = hashIndex.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)

	// 重复put同一个key
	err = hashIndex.Put([]byte("key1"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	err = hashIndex.Put([]byte("key1"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	err = hashIndex.Put([]byte("key1"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
}

// 并发写入
func TestHash_Put2(t *testing.T) {
	var wg sync.WaitGroup

	hashIndex := newHashIndex()

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func(pos int) {
			defer wg.Done()
			err := hashIndex.Put([]byte(strconv.Itoa(pos)),
				&data.RecordPos{FileID: 1, Offset: pos})
			assert.Nil(t, err)
		}(i)
	}

	pos, err := hashIndex.Get([]byte("0"))
	assert.Nil(t, err)
	assert.Equal(t, &data.RecordPos{FileID: 1, Offset: 0}, pos)

	wg.Wait()
}

func TestHash_Get(t *testing.T) {
	hashIndex := newHashIndex()

	// hash中map为空
	value, err := hashIndex.Get([]byte("k1"))
	assert.NotNil(t, err)
	assert.Nil(t, value)

	// 先写入数据 再读取
	err = hashIndex.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	pos, err := hashIndex.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, &data.RecordPos{FileID: 1, Offset: 10}, pos)

	// 重复put同一个key
	err = hashIndex.Put([]byte("key2"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	err = hashIndex.Put([]byte("key2"), &data.RecordPos{FileID: 1, Offset: 20})
	assert.Nil(t, err)
	pos, err = hashIndex.Get([]byte("key2"))
	assert.Nil(t, err)
	assert.Equal(t, &data.RecordPos{FileID: 1, Offset: 20}, pos)
}

func TestHash_Delete(t *testing.T) {
	hashIndex := newHashIndex()

	// 删除一个不存在的元素
	err := hashIndex.Delete([]byte("key"))
	assert.Nil(t, err)

	// put一个key再删除
	err = hashIndex.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	pos, err := hashIndex.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, &data.RecordPos{FileID: 1, Offset: 10}, pos)
	err = hashIndex.Delete([]byte("key"))
	assert.Nil(t, err)
	pos, err = hashIndex.Get([]byte("key"))
	assert.NotNil(t, err)
}

// TODO
func TestHash_Iterator(t *testing.T) {
	hash := map[string]*data.RecordPos{
		"k1": &data.RecordPos{
			FileID: 1,
			Offset: 10,
		},
		"k2": &data.RecordPos{
			FileID: 1,
			Offset: 20,
		},
		"k3": &data.RecordPos{
			FileID: 1,
			Offset: 30,
		},
	}
	it := newHashIterator(hash, false)
	fmt.Println(string(it.Key()))
	it.Next()
	fmt.Println(string(it.Key()))
}
