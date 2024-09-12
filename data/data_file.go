package data

type DataFile struct {
	FileID      int
	Offset      int
	WriteOffSet int
}

func OpenDataFile(dirPath string, fileID int) (*DataFile, error) {
	return nil, nil
}

func (d *DataFile) Sync() error {
	return nil
}

func (d *DataFile) Write(buf []byte) error {
	return nil
}

func (d *DataFile) ReadRecord(offset int) (*RecordInfo, int, error) {
	return nil, 0, nil
}
