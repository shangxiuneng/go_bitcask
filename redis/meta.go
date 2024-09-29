package redis

const (
	maxMetaDataSize   = 1
	extraListMetaSize = 1
)

// 元数据
type metaData struct {
	dataType byte // 数据类型
}

// 编码meta
func encodeMetaData() []byte {
	return nil
}

func decodeMetaData() *metaData {
	return nil
}
