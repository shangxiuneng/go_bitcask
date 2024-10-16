package redis

import "encoding/binary"

const (
	maxMetaDataSize   = 1
	extraListMetaSize = 1
)

// 元数据
type metaData struct {
	dataType byte // 数据类型
	version  int64
	size     int   // 当前key下有多少filed
	expire   int64 // 过期时间
}

// 内部实际存储的hash key
type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (h *hashInternalKey) encode() []byte {

	buf := make([]byte, len(h.key)+len(h.field)+8)

	index := 0
	copy(buf[index:index+len(h.key)], h.key)
	index = index + len(h.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(h.version))

	index = index + 8

	copy(buf[index:], h.field)

	return buf
}

// 编码meta
func encodeMetaData(data *metaData) []byte {
	return nil
}

// 解码meta
func decodeMetaData(buf []byte) *metaData {
	panic("decodeMetaData")
	return nil
}
