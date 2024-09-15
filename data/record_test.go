package data

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

/*
Key        []byte
Value      []byte
RecordType byte // 记录的类型
*/
func TestEncodeRecord(t *testing.T) {
	rec1 := &RecordInfo{
		Key:   []byte("1"),
		Value: []byte("hello world"),
		Type:  1,
	}
	res1, n1 := EncodeRecord(rec1)
	assert.NotNil(t, res1)
	assert.Equal(t, n1, 19)
	wantRes := []byte{167, 191, 229, 114, 1, 2, 22, 49, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100}
	assert.Equal(t, res1, wantRes)
}

func TestGetRecordCRC(t *testing.T) {

}

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  byte
}

const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化一个 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节存储 Type
	header[4] = logRecord.Type
	var index = 5
	// 5 字节之后，存储的是 key 和 value 的长度信息
	// 使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	// 将 header 部分的内容拷贝过来
	copy(encBytes[:index], header[:index])
	// 将 key 和 value 数据拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对整个 LogRecord 的数据进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}
