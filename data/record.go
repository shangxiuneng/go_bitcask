package data

import (
	"encoding/binary"
	"hash/crc32"
)

type RecordPos struct {
	FileID int // 文件id
	Offset int // 在文件中的偏移量
}

type RecordInfo struct {
	Key   []byte
	Value []byte
	Type  byte // 记录的类型
}

type RecordHeader struct {
	crc        uint32 // crc校验
	recordType byte   // 记录类型
	keySize    uint32
	valueSize  uint32
}

// TODO 这里为什么是5
// crc type keySize valueSize
// 4   1     5         5
const maxRecordSize = 15

// EncodeRecord 对数据信息进行编码
/*
crc  type     keySize valueSize key value
4字节 1字节    不确定    不确定
*/
func EncodeRecord(recordInfo *RecordInfo) ([]byte, int) {
	headerBuf := make([]byte, maxRecordSize)

	headerBuf[4] = recordInfo.Type

	index := 5
	n := binary.PutVarint(headerBuf[index:], int64(len(recordInfo.Key)))
	index = index + n

	n = binary.PutVarint(headerBuf[index:], int64(len(recordInfo.Value)))
	index = index + n

	totalSize := index + len(recordInfo.Key) + len(recordInfo.Value)

	recordBuf := make([]byte, totalSize)

	// 复制header
	copy(recordBuf[:index], headerBuf[:index])

	// 复制key
	copy(recordBuf[:index], recordInfo.Key)
	// 复制value
	copy(recordBuf[:index+len(recordInfo.Key)], recordInfo.Value)

	crc := crc32.ChecksumIEEE(recordBuf[4:])

	binary.LittleEndian.PutUint32(recordBuf[4:], crc)

	return recordBuf, totalSize
}

/*
crc  type     keySize valueSize
4    1        变长     变长

	crc        uint32 // crc校验
	recordType byte   // 记录类型
	keySize    uint32
	valueSize  uint32
*/
func decodeRecordHeader(buf []byte) (*RecordHeader, int) {
	if len(buf) <= 4 {
		return nil, 0
	}

	// crc校验
	crc := binary.LittleEndian.Uint32(buf[:4])

	// recordType
	recordType := buf[4]

	index := 5
	keySize, n := binary.Varint(buf[index:])
	index = index + n

	valueSize, n := binary.Varint(buf[index:])
	index = index + n

	return &RecordHeader{
		crc:        crc,
		recordType: recordType,
		keySize:    uint32(keySize),
		valueSize:  uint32(valueSize),
	}, index
}

// TODO encodeHeader的时候 crc校验无法获取
func encodeRecordHeader(header *RecordHeader) ([]byte, error) {
	headerBuf := make([]byte, maxRecordSize)

	headerBuf[4] = header.recordType
	index := 5
	n := binary.PutVarint(headerBuf[index:], int64(header.keySize))
	index = index + n
	n = binary.PutVarint(headerBuf[index:], int64(header.valueSize))
	index = index + n

	return nil, nil
}

/*
校验数据的有效性 不用校验前4个字节
*/
func GetRecordCRC(r *RecordInfo, headerBuf []byte) uint32 {

	if r == nil {
		return 0
	}

	buf := make([]byte, len(headerBuf)+len(r.Key)+len(r.Value))

	return crc32.ChecksumIEEE(buf)

}
