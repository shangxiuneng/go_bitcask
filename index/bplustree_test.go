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
