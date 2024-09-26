package fio

import (
	"github.com/rs/zerolog/log"
	"os"
)

type FileIO struct {
	fd *os.File
}

func NewFileIO(fileName string) (IOManager, error) {
	// 文件不存在 则创建对应的文件
	fd, err := os.OpenFile(fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644)
	if err != nil {
		log.Error().Msgf("NewFileIO error,err = %v", err)
		return nil, err
	}
	return &FileIO{
		fd: fd,
	}, nil
}

// 从文件中读取数据
func (f *FileIO) Read(b []byte, offset int64) (int, error) {
	return f.fd.ReadAt(b, offset)
}

// 向文件中写入数据
func (f *FileIO) Write(data []byte) (int, error) {
	return f.fd.Write(data)
}

// Sync 持久化到磁盘
func (f *FileIO) Sync() error {
	return f.fd.Sync()
}

func (f *FileIO) Close() error {
	return f.fd.Close()
}

// Size 获取当前文件的大小
func (f *FileIO) Size() (int, error) {
	fileInfo, err := f.fd.Stat()

	if err != nil {
		log.Error().Msgf("size error,err = %v", err)
		return 0, err
	}

	return int(fileInfo.Size()), nil
}
