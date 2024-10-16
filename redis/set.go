package redis

import (
	"github.com/rs/zerolog/log"
	"go_bitcask"
)

func (s *Service) SAdd(key, member []byte) (bool, error) {
	// 查找源数据
	meta, err := s.findMetaData(key, Set)
	if err != nil {
		log.Error().Msgf("findMetaData error,err = %v", err)
		return false, err
	}

	// 构造数据部分的key
	setKey := setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	ok := false
	if _, err := s.db.Get(setKey.encode()); err != nil {
		writeBatch := s.db.NewWriteBatch(go_bitcask.DefaultBatchConfig)
		meta.size++
		writeBatch.Put(key, encodeMetaData(meta))
		writeBatch.Put(setKey.encode(), nil)
		if err = writeBatch.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

// SIsMember 判断成员是否是集合的成员
func (s *Service) SIsMember(key, member []byte) (bool, error) {
	// 查找源数据
	meta, err := s.findMetaData(key, Set)
	if err != nil {
		log.Error().Msgf("findMetaData error,err = %v", err)
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	// 构造数据部分的key
	setKey := setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := s.db.Get(setKey.encode())
	if err != nil {
		return false, err
	}

	if len(value) == 0 {
		return false, nil
	}

	return true, nil
}

// SRem 用于移除集合中的一个或多个成员元素
func (s *Service) SRem(key, member []byte) (bool, error) {
	// 查找源数据
	meta, err := s.findMetaData(key, Set)
	if err != nil {
		log.Error().Msgf("findMetaData error,err = %v", err)
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	// 构造数据部分的key
	setKey := setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err := s.db.Get(setKey.encode()); err != nil {
		// 说明key不存在
		return false, nil
	}

	writeBatch := s.db.NewWriteBatch(go_bitcask.DefaultBatchConfig)

	meta.size--

	writeBatch.Put(key, encodeMetaData(meta))
	writeBatch.Delete(setKey.encode())

	if err := writeBatch.Commit(); err != nil {
		return false, err
	}

	return true, nil
}
