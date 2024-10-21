package index

import (
	"github.com/stretchr/testify/assert"
	"go_bitcask/data"
	"strconv"
	"sync"
	"testing"
)

func TestArtTree_Put(t *testing.T) {
	artIndex := newArtTree()
	// key为nil
	err := artIndex.Put(nil, &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	t.Logf("err = %v", err)

	// key正常
	err = artIndex.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)

	// 重复put同一个key
	err = artIndex.Put([]byte("key1"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	err = artIndex.Put([]byte("key1"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	err = artIndex.Put([]byte("key1"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
}

func TestArtTree_Put2(t *testing.T) {
	var wg sync.WaitGroup

	artIndex := newArtTree()

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func(pos int) {
			defer wg.Done()
			err := artIndex.Put([]byte(strconv.Itoa(pos)),
				&data.RecordPos{FileID: 1, Offset: pos})
			assert.Nil(t, err)
		}(i)
	}

	pos, err := artIndex.Get([]byte("0"))
	assert.Nil(t, err)
	assert.Equal(t, &data.RecordPos{FileID: 1, Offset: 0}, pos)

	wg.Wait()
}

func TestArtTree_Get(t *testing.T) {
	artIndex := newArtTree()

	value, err := artIndex.Get([]byte("k1"))
	assert.Nil(t, err)
	assert.Nil(t, value)

	// 先写入数据 再读取
	err = artIndex.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	pos, err := artIndex.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, &data.RecordPos{FileID: 1, Offset: 10}, pos)

	// 重复put同一个key
	err = artIndex.Put([]byte("key2"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	err = artIndex.Put([]byte("key2"), &data.RecordPos{FileID: 1, Offset: 20})
	assert.Nil(t, err)
	pos, err = artIndex.Get([]byte("key2"))
	assert.Nil(t, err)
	assert.Equal(t, &data.RecordPos{FileID: 1, Offset: 20}, pos)
}

func TestArtTree_Delete(t *testing.T) {
	artIndex := newArtTree()

	// 删除一个不存在的元素
	err := artIndex.Delete([]byte("key"))
	assert.Nil(t, err)

	// put一个key再删除
	err = artIndex.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	pos, err := artIndex.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, &data.RecordPos{FileID: 1, Offset: 10}, pos)
	err = artIndex.Delete([]byte("key"))
	assert.Nil(t, err)
	pos, err = artIndex.Get([]byte("key"))
	assert.Nil(t, err)
}

func TestArtTree_Iterator(t *testing.T) {
	artIndex := newArtTree()

	artIndex.Put([]byte("ccde"), &data.RecordPos{FileID: 1, Offset: 12})
	artIndex.Put([]byte("adse"), &data.RecordPos{FileID: 1, Offset: 12})
	artIndex.Put([]byte("bbde"), &data.RecordPos{FileID: 1, Offset: 12})
	artIndex.Put([]byte("bade"), &data.RecordPos{FileID: 1, Offset: 12})

	iter := artIndex.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
