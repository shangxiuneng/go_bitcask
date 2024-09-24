package go_bitcask

import (
	"github.com/rs/zerolog/log"
	"go_bitcask/data"
	"sync"
	"sync/atomic"
)

// WriteBatch 批量的事务操作
type WriteBatch struct {
	lock      *sync.Mutex
	db        *DB
	config    BatchConfig                 // 批量写的配置
	kvMapping map[string]*data.RecordInfo // golang的map key类型不能是[]byte 因此转为string
}

var (
	txKey      = []byte("txn-fin")
	noTrxSeqNo = int32(0) // 非事务的操作
)

func NewWriteBatch(db *DB, config BatchConfig) WriteBatch {
	return WriteBatch{
		lock:      new(sync.Mutex),
		config:    config,
		kvMapping: make(map[string]*data.RecordInfo),
		db:        db,
	}
}

// Put 写入数据
func (w *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 || w.kvMapping == nil {
		return ErrKeyIsNil
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	recordInfo := data.RecordInfo{
		Key:   key,
		Value: value,
		Type:  1,
	}

	w.kvMapping[string(key)] = &recordInfo

	return nil
}

// Delete 删除数据
func (w *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 || w.kvMapping == nil {
		return ErrKeyIsNil
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	pos, _ := w.db.index.Get(key)
	if pos == nil {
		if _, ok := w.kvMapping[string(key)]; ok {
			delete(w.kvMapping, string(key))
		}
	} else {
		recordInfo := data.RecordInfo{
			Key:  key,
			Type: 2,
		}

		w.kvMapping[string(key)] = &recordInfo
	}
	return nil
}

// Commit 提交事务
func (w *WriteBatch) Commit() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(w.kvMapping) == 0 {
		return nil
	}

	w.db.lock.Lock()
	defer w.db.lock.Unlock()

	seqNo := atomic.AddInt32(&w.db.seqNo, 1)

	// 写数据到文件中
	positionMapping := make(map[string]*data.RecordPos, 0)
	for _, v := range w.kvMapping {
		pos, err := w.db.appendRecord(&data.RecordInfo{
			Key:   encodeKeyWithSeqNo(v.Key, seqNo),
			Value: v.Value,
			Type:  1,
		})
		if err != nil {
			return err
		}

		positionMapping[string(v.Key)] = pos
	}

	// 写一条标记事务完成的标记
	txRecordInfo := &data.RecordInfo{
		Key:  txKey,
		Type: 3,
	}

	if _, err := w.db.appendRecord(txRecordInfo); err != nil {
		log.Error().Msgf("appendRecord error,err = %v", err)
		return err
	}

	// 根据配置决定是否持久化
	if w.config.SyncWrite && w.db.activeFile != nil {
		if err := w.db.activeFile.Sync(); err != nil {
			log.Error().Msgf("Sync error,err = %v", err)
			return err
		}
	}

	// 更新内存中的索引信息
	for _, v := range w.kvMapping {
		if v.Type == 1 {
			// 更新索引
			w.db.index.Put(v.Key, positionMapping[string(v.Key)])
		}

		if v.Type == 2 {
			w.db.index.Delete(v.Key)
		}
	}

	w.kvMapping = make(map[string]*data.RecordInfo)

	return nil
}

// 编码后的key
func encodeKeyWithSeqNo(key []byte, seqNo int32) []byte {
	return nil
}

// 返回值 key seqNo
func parseKeyWithSeqNo(key []byte) ([]byte, int32) {
	return nil, 0
}

// RollBack 回滚
func (w *WriteBatch) RollBack() {
	w.kvMapping = nil
	return
}
