package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

/*
case
1. 正常情况
2. tx的记录
3. 删除的记录
4. value为nil
5. key为nil
*/
func TestEncodeRecord(t *testing.T) {

	tests := []struct {
		param      *RecordInfo
		result     []byte
		encodeSize int
	}{
		{
			param: &RecordInfo{
				Key:   []byte("1"),
				Value: []byte("hello world"),
				Type:  NormalRecord,
			},
			result:     []byte{167, 191, 229, 114, 1, 2, 22, 49, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100},
			encodeSize: 19,
		},
		{
			param: &RecordInfo{
				Key:   []byte("1"),
				Value: nil,
				Type:  DeleteRecord,
			},
			result:     []byte{195, 195, 23, 217, 2, 2, 0, 49},
			encodeSize: 8,
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			dataRecord, size := EncodeRecord(tt.param)
			assert.Equal(t, tt.encodeSize, size)
			assert.Equal(t, tt.result, dataRecord)
			// fmt.Println(dataRecord)
		})
	}
}

func TestGetRecordCRC(t *testing.T) {
	buf := []byte{167, 191, 229, 114, 1, 2, 22, 49, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100}
	headerBuf := []byte{167, 191, 229, 114, 1, 2, 22}

	r := &RecordInfo{
		Key:   buf[len(headerBuf) : len(headerBuf)+1],
		Value: buf[len(headerBuf)+1:],
	}
	gotCRC := GetRecordCRC(r, headerBuf[crc32.Size:len(headerBuf)])

	assert.Equal(t, uint32(1927659431), gotCRC)
}

func TestDecodeRecordHeader(t *testing.T) {
	buf := []byte{167, 191, 229, 114, 1, 2, 22, 49, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100}
	headerInfo, size := decodeRecordHeader(buf)

	assert.NotNil(t, headerInfo)
	assert.Equal(t, headerInfo.keySize, uint32(1))
	assert.Equal(t, headerInfo.valueSize, uint32(11))
	assert.Equal(t, headerInfo.crc, uint32(1927659431)) // 1927659431
	assert.Equal(t, size, 7)
}

func TestEncodeRecordPos(t *testing.T) {
	pos := &RecordPos{
		FileID: 1,
		Offset: 100,
	}

	posData := EncodeRecordPos(pos)
	gotPos, err := DecodeRecordPos(posData)
	assert.Nil(t, err)
	assert.Equal(t, pos, gotPos)
}
