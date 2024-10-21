package redis

import (
	"github.com/stretchr/testify/assert"
	"go_bitcask"
	"testing"
)

func TestService_HGet_HSet(t *testing.T) {
	config := go_bitcask.Config{
		DirPath:      "temp",
		DataFileSize: 1000,
	}
	redis, err := NewRedisService(config)
	assert.Nil(t, err)
	assert.NotNil(t, redis)

	ok, err := redis.HSet([]byte("key"), []byte("filed1"), []byte("value1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = redis.HSet([]byte("key"), []byte("filed2"), []byte("value2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	value, err := redis.HGet([]byte("key"), []byte("filed1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), value)
}

func TestService_HDel(t *testing.T) {

}
