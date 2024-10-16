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
	size     int    // 当前key下有多少filed
	expire   int64  // 过期时间
	head     uint64 // list使用 链表的头部
	tail     uint64 // list使用 链表的尾部
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

type setInternalKey struct {
	key        []byte
	version    int64
	member     []byte
	memberSize int64
}

func (s *setInternalKey) encode() []byte {
	buf := make([]byte, len(s.key)+len(s.member)+8+8)

	// key
	index := 0
	copy(buf[index:index+len(s.key)], s.key)
	index = index + len(s.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(s.version))

	index = index + 8

	// member
	copy(buf[index:], s.member)
	index = index + len(s.member)

	// member size
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(s.memberSize))

	index = index + 8

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (l *listInternalKey) encode() []byte {
	buf := make([]byte, len(l.key)+8+8)

	index := 0

	copy(buf[:len(l.key)], l.key)
	index = index + len(buf)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(l.version))
	index = index + 8

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(l.version))
	index = index + 8

	return buf
}
