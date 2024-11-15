package data

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type RecordPos struct {
	FileID int // 文件id
	Offset int // 在文件中的偏移量
	Size   int // RecordPos的大小
}

type RecordType byte

const (
	NormalRecord      = 1
	DeleteRecord      = 2
	TransactionRecord = 3 // 事务Record的标识
)

type RecordInfo struct {
	Key   []byte
	Value []byte
	Type  RecordType // 记录的类型
}

type TrxRecord struct {
	RecordInfo *RecordInfo
	Pos        *RecordPos
}

type RecordHeader struct {
	crc        uint32     // crc校验
	recordType RecordType // 记录类型
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

	headerBuf[4] = byte(recordInfo.Type)

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
	copy(recordBuf[index:], recordInfo.Key)
	// 复制value
	copy(recordBuf[index+len(recordInfo.Key):], recordInfo.Value)

	crc := crc32.ChecksumIEEE(recordBuf[4:])

	binary.LittleEndian.PutUint32(recordBuf[:4], crc)

	return recordBuf, totalSize
}

/*
crc  type     keySize valueSize
4    1        变长     变长

	crc        uint32 // crc校验
	recordType byte   // 记录类型
	keySize    uint32 //
	valueSize  uint32  //
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
		recordType: RecordType(recordType),
		keySize:    uint32(keySize),
		valueSize:  uint32(valueSize),
	}, index
}

// TODO encodeHeader的时候 crc校验无法获取
func encodeRecordHeader(header *RecordHeader) ([]byte, error) {
	headerBuf := make([]byte, maxRecordSize)

	headerBuf[4] = byte(header.recordType)
	index := 5
	n := binary.PutVarint(headerBuf[index:], int64(header.keySize))
	index = index + n
	n = binary.PutVarint(headerBuf[index:], int64(header.valueSize))
	index = index + n

	return nil, nil
}

// GetRecordCRC 校验数据的有效性 不用校验前4个字节
func GetRecordCRC(r *RecordInfo, headerBuf []byte) uint32 {

	if r == nil {
		return 0
	}

	// 构造一个buf
	// 把数据都copy到buf中
	// 返回crc的校验值即可
	buf := make([]byte, len(headerBuf)+len(r.Key)+len(r.Value))

	copy(buf, headerBuf)
	copy(buf[len(headerBuf):], r.Key)
	copy(buf[len(headerBuf)+len(r.Key):], r.Value)

	return crc32.ChecksumIEEE(buf)

}

// EncodeRecordPos 对RecordPos进行编码
func EncodeRecordPos(pos *RecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2)
	index := 0
	n := binary.PutVarint(buf[index:], int64(pos.FileID))
	index = index + n
	n = binary.PutVarint(buf[index:], int64(pos.Offset))
	index = index + n
	return buf[:index]
}

func DecodeRecordPos(posData []byte) (*RecordPos, error) {
	pos := &RecordPos{}
	index := 0

	fileID, n := binary.Varint(posData[index:])
	if n <= 0 {
		return nil, fmt.Errorf("failed to decode FileID")
	}
	pos.FileID = int(fileID)
	index += n

	offset, n := binary.Varint(posData[index:])
	if n <= 0 {
		return nil, fmt.Errorf("failed to decode Offset")
	}
	pos.Offset = int(offset)

	return pos, nil
}
