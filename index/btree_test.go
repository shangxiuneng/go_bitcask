package index

import (
	"github.com/stretchr/testify/assert"
	"go_bitcask/data"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	btreeIndex := NewIndex(2, "", false)

	// key为nil
	err := btreeIndex.Put(nil, &data.RecordPos{FileID: 1, Offset: 10})
	assert.NotNil(t, err)
	t.Logf("err = %v", err)

	// key正常
	err = btreeIndex.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
	assert.Nil(t, err)
}

func TestBTree_Delete(t *testing.T) {
	btreeIndex := NewIndex(2, "", false)

	err := btreeIndex.Delete(nil)
	assert.NotNil(t, err)
	t.Logf("err = %v", err)
}
