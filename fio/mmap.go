package fio

import "golang.org/x/exp/mmap"

// MMap  只有在启动时才使用mmap
type MMap struct {
	IOManager

	readerAt *mmap.ReaderAt
}

func newMMap(fileName string) (IOManager, error) {
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{
		readerAt: readerAt,
	}, nil
}

func (m *MMap) Read(b []byte, offset int64) (int, error) {
	// 读取文件内容
	n, err := m.readerAt.ReadAt(b, offset)
	return n, err
}
func (m *MMap) Write(data []byte) (int, error) {
	return 0, nil
}
func (m *MMap) Sync() error {
	return nil
}
func (m *MMap) Close() error {
	return m.readerAt.Close()
}
func (m *MMap) Size() (int, error) {
	return 0, nil
}
