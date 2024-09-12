package go_bitcask

import "go_bitcask/index"

type Config struct {
	DirPath      string
	DataFileSize int             // 每一个文件的大小
	SyncWrite    bool            // 是否每次写完都进行持久化配置
	IndexType    index.IndexType // 索引类型
}
