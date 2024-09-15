package data

import (
	"github.com/stretchr/testify/assert"
	"io/fs"
	"os"
	"testing"
)

func TestNewDataFile(t *testing.T) {
	// dirPath为空
	dataFile, err := NewDataFile("", 1)
	assert.NotNil(t, err)
	assert.Nil(t, dataFile)

	// 正常测试用例
	err = os.Mkdir("temp", fs.ModeDir)
	assert.Nil(t, err)
	dataFile1, err := NewDataFile("temp", 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	// TODO 测试完成后需要删除文件夹
	//os.RemoveAll("/temp")
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := NewDataFile("temp", 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = dataFile.Write(nil)
	assert.Nil(t, err)

	err = dataFile.Write([]byte("nihao"))
	assert.Nil(t, err)
}

func TestDataFile_ReadRecord(t *testing.T) {
	dataFile, err := NewDataFile("temp", 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 写入一条记录
	recordInfo := &RecordInfo{
		Key:   []byte("1"),
		Value: []byte("hello world"),
		Type:  1,
	}
	recordBuf, size := EncodeRecord(recordInfo)

	err = dataFile.Write(recordBuf)
	assert.Nil(t, err)

	// 读出记录
	gotRecordInfo, gotSize, err := dataFile.ReadRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, gotSize, size)
	assert.Equal(t, gotRecordInfo.Key, recordInfo.Key)
}
