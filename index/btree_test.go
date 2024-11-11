package index

//
//import (
//	"github.com/stretchr/testify/assert"
//	"go_bitcask/data"
//	"testing"
//)
//
//func TestBTree_Put(t *testing.T) {
//	btreeIndex := NewIndex(2, "", false)
//
//	// key为nil
//	err := btreeIndex.Put(nil, &data.RecordPos{FileID: 1, Offset: 10})
//	assert.NotNil(t, err)
//	t.Logf("err = %v", err)
//
//	// key正常
//	err = btreeIndex.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
//	assert.Nil(t, err)
//}
//
//func TestBTree_Get(t *testing.T) {
//	btreeIndex := newBTree(2)
//
//	// 定义测试用例
//	tests := []struct {
//		ParamKey []byte
//		ParamPos *data.RecordPos
//	}{
//		{
//			ParamKey: []byte("key1"),
//			ParamPos: &data.RecordPos{
//				FileID: 1,
//				Offset: 10,
//			},
//		},
//		{
//			ParamKey: []byte("key2"),
//			ParamPos: &data.RecordPos{
//				FileID: 1,
//				Offset: 20,
//			},
//		},
//	}
//
//	// 遍历测试用例
//	for _, tt := range tests {
//		t.Run("", func(t *testing.T) {
//			err := btreeIndex.Put(tt.ParamKey, tt.ParamPos)
//			assert.Nil(t, err)
//			got, err := btreeIndex.Get(tt.ParamKey)
//			assert.Nil(t, err)
//			assert.Equal(t, tt.ParamPos, got)
//		})
//	}
//}
//
//// put重复值 然后读取
//func TestBTree_Get1(t *testing.T) {
//	btreeIndex := newBTree(2)
//
//	// key正常
//	err := btreeIndex.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
//	assert.Nil(t, err)
//
//	err = btreeIndex.Put([]byte("key"), &data.RecordPos{FileID: 2, Offset: 20})
//	assert.Nil(t, err)
//	pos, err := btreeIndex.Get([]byte("key"))
//	assert.Nil(t, err)
//	assert.Equal(t, &data.RecordPos{FileID: 2, Offset: 20}, pos)
//}
//
//func TestBTree_Delete(t *testing.T) {
//	btreeIndex := newBTree(2)
//
//	// key为nil
//	err := btreeIndex.Delete(nil)
//	assert.NotNil(t, err)
//
//	// 删除一个不存在的元素
//	err = btreeIndex.Delete([]byte("key1"))
//	assert.Nil(t, err)
//
//	// put元素后删除
//	err = btreeIndex.Put([]byte("key"), &data.RecordPos{FileID: 1, Offset: 10})
//	assert.Nil(t, err)
//	// 删除元素
//	err = btreeIndex.Delete([]byte("key"))
//	assert.Nil(t, err)
//	// 获取元素
//	pos, err := btreeIndex.Get([]byte("key"))
//	assert.Nil(t, err)
//	var want *data.RecordPos
//	assert.Equal(t, want, pos)
//}
