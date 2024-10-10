package redis

const (
	maxMetaDataSize   = 1
	extraListMetaSize = 1
)

// 元数据
type metaData struct {
	dataType byte // 数据类型
	version  int64
	size     int // 当前key下有多少filed
}

// 内部实际存储的hash key
type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (h *hashInternalKey) encode() []byte {
	return nil
}

// 编码meta  // TODO 函数的名字需要改一下
func (m *metaData) encodeMetaData() []byte {
	return nil
}

func (m *metaData) decodeMetaData() *metaData {
	return nil
}
