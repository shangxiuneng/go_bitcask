package redis

import "go_bitcask"

/*
hash结构支持
key = type + expire + version + size
expire 过期时间
version 用于快速删除
size 当前key下有多少field
value = value
*/

func (s *Service) HSet(key []byte, filed []byte, value []byte) (bool, error) {
	meta, err := s.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造hash的key
	internalKey := hashInternalKey{
		key:     filed,
		version: meta.version,
		field:   filed,
	}
	internalKeyData := internalKey.encode()

	exist := true
	if _, err = s.db.Get(internalKeyData); err != nil {
		// TODO 对 error的判断太粗略
		exist = false
	}

	// TODO new方法实现的不好 应该把new放在db上
	writeBatch := s.db.NewWriteBatch(go_bitcask.BatchConfig{})
	if !exist {
		// 说明key不存在
		meta.size++
		_ = writeBatch.Put(key, meta.encodeMetaData())
	}

	_ = writeBatch.Put(internalKeyData, value)

	if err := writeBatch.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) HGet(key []byte) ([]byte, error) {
	meta, err := s.findMetaData(key, Hash)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	// TODO
	return nil, nil
}

func (s *Service) HDel(key []byte, filed []byte, value []byte) (bool, error) {
	return false, nil
}
