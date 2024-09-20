package go_bitcask

import "go_bitcask/index"

type Config struct {
	DirPath      string
	DataFileSize int             // 每一个文件的大小
	SyncWrite    bool            // 是否每次写完都进行持久化配置
	IndexType    index.IndexType // 索引类型
}

type IteratorConfig struct {
	// 遍历前缀为指定值的key 默认为空
	Prefix []byte

	// 是否反向遍历  默认为false 正向遍历
	Reverse bool
}