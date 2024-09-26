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

var (
	// MergeFinFileName merge结束的文件
	MergeFinFileName = "merge_fin_file"
	SeqNoFileName    = "seq-no-file"
	// DataFileNameSuffix 数据文件的后缀
	DataFileNameSuffix = ".data"
)

// NewDataFile 返回数据文件
func NewDataFile(dirPath string, fileID int) (*DataFile, error) {
	if dirPath == "" {
		return nil, errors.New("文件路径为空")
	}

	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileID)+DataFileNameSuffix)

	return newDataFile(fileName, fileID)
}

// GetDataFileName 获取数据文件名
func GetDataFileName(dirPath string, fileID int) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileID)+DataFileNameSuffix)
}

// NewHintFile 打开一个hint文件
func NewHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath)
	return newDataFile(fileName, 0)
}

// NewMergeFinFile 打开一个merge文件  TODO 未实现
func NewMergeFinFile(dirPath string) (*DataFile, error) {
	return nil, nil
}

func newDataFile(fileName string, fileID int) (*DataFile, error) {

	log.Info().Msgf("fileName = %v", fileName)

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

func NewSeqNumFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0)
}

func (d *DataFile) Sync() error {
	return d.IOManager.Sync()
}

func (d *DataFile) Close() error {
	return d.IOManager.Close()
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
		Type: RecordType(recordHeader.recordType),
	}

	// 读取用户实际存储的key value数据
	realKVBuf, err := d.readFromOffset(keySize+valueSize, offset+headerSize)
	if err != nil {
		log.Error().Msgf("ReadRecord error,err = %v", err)
		return nil, 0, err
	}

	record.Key = realKVBuf[:keySize]
	record.Value = realKVBuf[keySize:]

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

func (d *DataFile) WriteHintFile(key []byte, pos *RecordPos) error {
	recordInfo := &RecordInfo{
		Key:   key,
		Value: EncodeRecordPos(pos),
	}

	recordData, _ := EncodeRecord(recordInfo)

	return d.Write(recordData)
}
