package data

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"go_bitcask/fio"
	"hash/crc32"
	"io"
	"path/filepath"
)

type DataFile struct {
	FileID      int
	WriteOffSet int
	IOManager   fio.IOManager
}

// NewDataFile 创建一个DataFile
func NewDataFile(dirPath string, fileID int) (*DataFile, error) {
	if dirPath == "" {
		return nil, errors.New("文件路径为空")
	}

	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileID)+".data")

	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		log.Error().Msgf("OpenDataFile error,err = %v", err)
		return nil, err
	}

	dataFile := DataFile{
		FileID:      fileID,
		WriteOffSet: 0,
		IOManager:   ioManager,
	}

	return &dataFile, nil
}

func (d *DataFile) Sync() error {
	return d.IOManager.Sync()
}

func (d *DataFile) Write(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	n, err := d.IOManager.Write(buf)
	if err != nil {
		log.Error().Msgf("Write error,err = %v", err)
		return err
	}

	d.WriteOffSet = d.WriteOffSet + n

	log.Log().Msgf("write success,n = %v", n)
	return nil
}

// ReadRecord 读取一条记录
func (d *DataFile) ReadRecord(offset int) (*RecordInfo, int, error) {
	fileSize, err := d.IOManager.Size()
	if err != nil {
		log.Error().Msgf("ioManager size error,err = %v", err)
		return nil, 0, err
	}

	if fileSize == 0 {
		return nil, 0, errors.New("当前文件为空")
	}

	headerSize := maxRecordSize

	if headerSize+offset > fileSize {
		headerSize = fileSize - offset
	}

	headerByte, err := d.readFromOffset(headerSize, offset)
	if err != nil {
		log.Error().Msgf("ReadRecord error,err = %v", err)
		return nil, 0, err
	}

	// 对header进行解码
	recordHeader, headerSize := decodeRecordHeader(headerByte)

	if recordHeader == nil {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int(recordHeader.keySize), int(recordHeader.valueSize)

	recordSize := keySize + valueSize + headerSize

	record := RecordInfo{
		Type: recordHeader.recordType,
	}

	// 读取用户实际存储的key value数据
	_, err = d.readFromOffset(keySize+valueSize, offset+headerSize)
	if err != nil {
		log.Error().Msgf("ReadRecord error,err = %v", err)
		return nil, 0, err
	}

	// TODO 校验数据的有效性
	crc := GetRecordCRC(&record, headerByte[crc32.Size:headerSize])

	if crc != recordHeader.crc {
		// 校验和出错
		log.Error().Msgf("crc error,crc = %v, %v", crc, recordHeader.crc)
		return nil, 0, errors.New("crc error")
	}

	return &record, recordSize, nil
}

// readFromOffset 从文件的offset位置读取n个长度
func (d *DataFile) readFromOffset(n int, offset int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := d.IOManager.Read(buf, int64(offset))
	return buf, err
}
