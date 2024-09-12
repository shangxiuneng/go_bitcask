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

// EncodeRecord 对数据信息进行编码
func EncodeRecord(recordInfo *RecordInfo) ([]byte, int) {
	return nil, 0
}
