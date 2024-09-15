package go_bitcask

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_Put(t *testing.T) {
	db, err := Open(Config{
		DirPath: "temp",
	})
	assert.Nil(t, err)

	db.Put([]byte("key"), []byte("value"))
}
