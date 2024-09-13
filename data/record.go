package data

type RecordPos struct {
	FileID int // 文件id
	Offset int // 在文件中的偏移量
}

type RecordInfo struct {
	Key        []byte
	Value      []byte
	RecordType int // 记录的类型
}

type RecordHeader struct {
	crc        uint32 // crc校验
	recordType int
	keySize    uint32
	valueSize  uint32
}

// TODO 这里为什么是5
// crc type keySize valueSize
// 4   1     5         5
const maxRecordSize = 15

// EncodeRecord 对数据信息进行编码
func EncodeRecord(recordInfo *RecordInfo) ([]byte, int) {
	return nil, 0
}

func decodeRecordHeader(buf []byte) (*RecordHeader, int) {
	return nil, 0
}

func getRecordCRC(r *RecordInfo, headerBuf []byte) uint32 {
	return 0
}
