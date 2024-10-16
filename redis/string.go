package redis

import (
	"encoding/binary"
	"errors"
	"github.com/rs/zerolog/log"
	"go_bitcask"
	"time"
)

type Service struct {
	db *go_bitcask.DB
}

func NewRedisService(config go_bitcask.Config) (*Service, error) {
	db, err := go_bitcask.Open(config)
	if err != nil {
		log.Error().Msgf("Open error,err = %v", err)
		return nil, err
	}

	return &Service{
		db: db,
	}, nil
}

// 查找元数据
func (s *Service) findMetaData(key []byte, dataType byte) (*metaData, error) {
	metaBuf, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}

	var meta *metaData
	exist := true
	if err != nil {
		exist = false
	} else {
		meta = decodeMetaData(metaBuf)
		if meta.dataType != dataType {
			return nil, errors.New("dataType error")
		}

		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metaData{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}

		if dataType == List {
			panic("List")
		}
	}

	return nil, nil
}

// Set string类型的set操作
func (s *Service) Set(key []byte, value []byte, ttl time.Duration) error {
	if value == nil {
		return nil
	}

	buf := make([]byte, binary.MaxVarintLen64+1)

	buf[0] = String

	index := 1
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
	expire, n := binary.Varint(payload[index:])
	index = index + n
	if expire > 0 && expire < time.Now().UnixNano() {
		return nil, nil
	}

	return payload[index:], nil
}
