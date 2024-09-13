package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

/*
Key        []byte
Value      []byte
RecordType byte // 记录的类型
*/
func TestEncodeRecord(t *testing.T) {
	rec1 := &RecordInfo{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  1,
	}
	res1, n1 := EncodeRecord(rec1)
	assert.NotNil(t, res1)
	assert.Equal(t, n1, int64(5))
}

func TestGetRecordCRC(t *testing.T) {

}