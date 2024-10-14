package redis

// 定义redis支持的数据类型
var (
	String = byte(1)
	List   = byte(2)
	Hash   = byte(3)
)
