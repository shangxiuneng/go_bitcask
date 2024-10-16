package redis

import "go_bitcask"

// LPush 将一个或多个值插入链表头部
func (s *Service) LPush(key, element []byte) (uint32, error) {
	return s.push(key, element, true)
}

func (s *Service) push(key, element []byte, isLeft bool) (uint32, error) {
	meta, err := s.findMetaData(key, List)
	if err != nil {
		return 0, err
	}

	listKey := listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isLeft {
		listKey.index = meta.head - 1
	} else {
		listKey.index = meta.tail
	}

	writeBatch := s.db.NewWriteBatch(go_bitcask.DefaultBatchConfig)

	writeBatch.Put(key, encodeMetaData(meta))
	writeBatch.Put(listKey.encode(), element)
	if err := writeBatch.Commit(); err != nil {
		return 0, err
	}
	meta.size++
	return uint32(meta.size), nil
}

func (s *Service) RPush(key, element []byte) (uint32, error) {
	return s.push(key, element, false)
}

func (s *Service) LPop(key []byte) ([]byte, error) {
	return s.pop(key, true)
}

func (s *Service) RPop(key []byte) ([]byte, error) {
	return s.pop(key, false)
}

func (s *Service) pop(key []byte, isLeft bool) ([]byte, error) {
	meta, err := s.findMetaData(key, List)
	if err != nil {
		return nil, err
	}

	listKey := listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isLeft {
		listKey.index = meta.head - 1
	} else {
		listKey.index = meta.tail
	}

	element, err := s.db.Get(listKey.encode())
	if err != nil {
		return nil, err
	}

	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}

	if err := s.db.Put(key, encodeMetaData(meta)); err != nil {
		return nil, err
	}

	meta.size--

	return element, nil
}
