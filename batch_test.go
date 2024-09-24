package go_bitcask

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteBatch_Put(t *testing.T) {
	db, err := Open(Config{
		DirPath:      "temp",
		DataFileSize: 1000,
	})
	assert.Nil(t, err)

	batch := NewWriteBatch(db, BatchConfig{})
	batch.Put([]byte("key1"), []byte("value"))
}
