package index

import (
	"github.com/stretchr/testify/assert"
	"go_bitcask/data"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	db := newBPlusTree("temp", false)
	assert.NotNil(t, db)

	// key为nil
	err := db.Put(nil, &data.RecordPos{FileID: 1, Offset: 10})
	assert.NotNil(t, err)
	t.Logf("err = %v", err)

	// key正常
	err = db.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
}

func TestBPlusTree_Get(t *testing.T) {
	db := newBPlusTree("temp", false)
	assert.NotNil(t, db)

	// key正常
	want := &data.RecordPos{FileID: 1, Offset: 10}
	err := db.Put([]byte("key"), want)
	assert.Nil(t, err)

	got, err := db.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestBPlusTree_Delete(t *testing.T) {
	db := newBPlusTree("temp", false)
	assert.NotNil(t, db)

	// 删除一个不存在的元素
	err := db.Delete([]byte("key"))
	assert.Nil(t, err)

	// put一个key再删除
	err = db.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
	pos, err := db.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, &data.RecordPos{FileID: 1, Offset: 10}, pos)
	err = db.Delete([]byte("key"))
	assert.Nil(t, err)
	pos, err = db.Get([]byte("key"))
	assert.Nil(t, err)
}

func TestBPlusTree_Iterator(t *testing.T) {
	db := newBPlusTree("temp", false)
	assert.NotNil(t, db)

	db.Put([]byte("ccde"), &data.RecordPos{FileID: 1, Offset: 12})
	db.Put([]byte("adse"), &data.RecordPos{FileID: 1, Offset: 12})
	db.Put([]byte("bbde"), &data.RecordPos{FileID: 1, Offset: 12})
	db.Put([]byte("bade"), &data.RecordPos{FileID: 1, Offset: 12})

	iter := db.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
