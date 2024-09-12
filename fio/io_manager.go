package fio

type IOManager interface {
	Read(b []byte, offset int64) (int, error)
	Write(data []byte) (int, error)
	Sync() error
	Close() error
}
