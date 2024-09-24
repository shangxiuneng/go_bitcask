package data

import (
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