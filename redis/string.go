package redis

import (
	"encoding/binary"
	"errors"
	"go_bitcask"
	"time"
)

type Service struct {
	db *go_bitcask.DB
}

func NewService() Service {
	return Service{}
}

func (s *Service) findMetaData(key []byte, dataType byte) (*metaData, error) {
	return nil, nil
}

// Set string类型的set操作
func (s *Service) Set(key []byte, value []byte, ttl time.Duration) error {
	if value == nil {
		return nil
	}

	/*
		编码value = type + ttl + value
	*/

	buf := make([]byte, binary.MaxVarintLen64+1)

	buf[0] = String

	index := 0
	expire := int64(0)

	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}

	index = index + binary.PutVarint(buf[index:], expire)

	payload := make([]byte, index+len(value))

	copy(payload[:index], buf[:index])
	copy(payload[index:], value)

	return s.db.Put(key, payload)
}

func (s *Service) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, nil
	}

	payload, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}

	dataType := payload[0]
	if dataType != String {
		return nil, errors.New("type error")
	}

	index := 1
	expire, n := binary.Varint(payload)
	index = index + n
	if expire > 0 && expire < time.Now().UnixNano() {
		return nil, nil
	}

	return payload[n:], nil
}
