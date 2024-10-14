package redis

import "errors"

func (s *Service) Del(key []byte) error {
	return s.db.Delete(key)
}

func (s *Service) Type(key []byte) (byte, error) {
	value, err := s.db.Get(key)
	if err != nil {
		return 0, err
	}

	if len(value) == 0 {
		return 0, errors.New("value is nil")
	}

	return value[0], nil
}
