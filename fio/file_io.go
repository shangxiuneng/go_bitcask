package fio

import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIO(fileName string) (IOManager, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
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
	return 0, nil
}
