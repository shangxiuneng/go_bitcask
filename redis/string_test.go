package redis

import (
	"github.com/stretchr/testify/assert"
	"go_bitcask"
	"testing"
)

func TestService_Get_Set(t *testing.T) {
	config := go_bitcask.Config{
		DirPath:      "temp",
		DataFileSize: 1000,
	}
	redis, err := NewRedisService(config)
	assert.Nil(t, err)
	assert.NotNil(t, redis)

	err = redis.Set([]byte("key"), []byte("value"), 0)
	assert.Nil(t, err)

	value, err := redis.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value"), value)
}
