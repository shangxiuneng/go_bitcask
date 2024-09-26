package fio

import "errors"

type FileIOType int

var (
	StandardIO FileIOType = 1
	MMApIO     FileIOType = 2
)

type IOManager interface {
	Read(b []byte, offset int64) (int, error)
	Write(data []byte) (int, error)
	Sync() error
	Close() error
	Size() (int, error)
}

func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardIO:
		return newFileIO(fileName)
	case MMApIO:
		return newMMap(fileName)
	default:
		return nil, errors.New("undefined io type")
	}

	return nil, nil
}
