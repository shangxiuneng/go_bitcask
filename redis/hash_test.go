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
	ok, err = redis.HSet([]byte("key"), []byte("filed12"), []byte("value12"))
	assert.Nil(t, err)
	assert.True(t, ok)
}

func TestService_HDel(t *testing.T) {

}
