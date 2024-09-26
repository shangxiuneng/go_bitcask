package go_bitcask

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go_bitcask/data"
	"go_bitcask/index"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	lock           *sync.Mutex
	activeFile     *data.DataFile         // 当前获取的文件
	fileMapping    map[int]*data.DataFile // 保存文件id到文件句柄的映射
	options        Config                 // 配置项
	index          index.Index            // 内存索引
	fileIDs        []int                  // 文件id 加载索引使用
	seqNo          int32                  // 事务编号
	isMerge        bool                   // 是否正在merge
	isSeqFileExist bool                   // 存储事务编号的文件是否存在
}

func Open(conf Config) (*DB, error) {
	if err := checkDBConfig(conf); err != nil {
		return nil, err
	}

	if _, err := os.Stat(conf.DirPath); os.IsNotExist(err) {
		// 创建目录
		if err := os.MkdirAll(conf.DirPath, os.ModePerm); err != nil {
			log.Error().Msgf("MkdirAll error,err = %v", err)
			return nil, err
		}
		log.Info().Msg("mkdir success")
	}

	db := &DB{
		options:     conf,
		lock:        new(sync.Mutex),
		fileMapping: map[int]*data.DataFile{},
		index:       index.NewIndex(index.BTreeIndex, "", false),
	}

	if err := db.loadMergeFile(); err != nil {
		log.Error().Msgf("loadMergeFile error,er = %v", err)
		return nil, err
	}

	// 加载对应的数据文件
	if err := db.loadDataFiles(); err != nil {
		log.Error().Msgf("loadDataFiles error,err = %v", err)
		return nil, err
	}

	if conf.IndexType != index.BPlusIndex {
		// b+树不从磁盘上加载索引
		// TODO 如果第一次使用的是hash 后面又改成了b+ 会有问题
		// 加载对应的hint文件
		if err := db.loadHintFile(); err != nil {
			log.Error().Msgf("loadHintFile error,err = %v", err)
			return nil, err
		}
		// 构造索引
		if err := db.loadIndex(); err != nil {
			log.Error().Msgf("loadIndex error,err = %v", err)
			return nil, err
		}
	}

	if conf.IndexType == index.BPlusIndex {
		// 加载seqNo

	}

	return db, nil
}

// 校验db的配置是否合理
func checkDBConfig(option Config) error {
	if option.DirPath == "" {
		return errors.New("dir is nil")
	}

	if option.DataFileSize <= 0 {
		return errors.New("data file size is 0")
	}

	return nil
}

// TODO 载入索引的过程会非常慢
func (d *DB) loadIndex() error {
	if len(d.fileIDs) == 0 {
		// 没有文件id
		return nil
	}

	currSeqNo := int32(0)
	trxRecordMapping := make(map[int32][]*data.TrxRecord)

	hasMerge := false
	noMergeFileID := 0

	mergeFileName := filepath.Join(d.options.DirPath, data.MergeFinFileName)
	_, err := os.Stat(mergeFileName)
	if !errors.Is(err, fs.ErrNotExist) {
		var err error
		noMergeFileID, err = d.getNoMergeFileID(mergeFileName)
		if err != nil {
			log.Error().Msgf("getNoMergeFileID error,err = %v", err)
			return err
		}
		hasMerge = true
	}

	fileIDs := d.fileIDs
	for i, fileID := range fileIDs {

		if hasMerge && fileID < noMergeFileID {
			continue
		}

		dataFile := &data.DataFile{}

		if fileID == d.activeFile.FileID {
			dataFile = d.activeFile
		} else {
			dataFile = d.fileMapping[fileID]
		}

		offset := 0

		// 循环读文件内容
		for {
			recordInfo, recordSize, err := dataFile.ReadRecord(offset)
			if err != nil {
				if err == io.EOF {
					// 说明读到文件末尾了
					log.Info().Msgf("io eof,fileID = %v", fileID)
					break
				}

				log.Error().Msgf("ReadRecord error,err = %v", err)
				return err
			}

			realKey, seqNo := parseKeyWithSeqNo(recordInfo.Key)
			// 不是事务操作 则直接更新内存索引
			pos := &data.RecordPos{
				FileID: fileID,
				Offset: offset,
			}

			if seqNo == noTrxSeqNo {
				d.updateIndex(realKey, recordInfo, pos)
			} else {
				if recordInfo.Type == 3 {
					// 事务完成的标志
					for _, v := range trxRecordMapping[seqNo] {
						d.updateIndex(v.RecordInfo.Key, v.RecordInfo, v.Pos)
					}
					delete(trxRecordMapping, seqNo)
				} else {
					trxRecordMapping[seqNo] = append(trxRecordMapping[seqNo], &data.TrxRecord{
						RecordInfo: recordInfo,
						Pos:        pos,
					})

				}
			}

			if seqNo > currSeqNo {
				currSeqNo = seqNo
			}

			offset = offset + recordSize

			if i == len(fileIDs)-1 {
				d.activeFile.WriteOffSet = offset
			}
		}
	}

	d.seqNo = currSeqNo
	return nil
}

func (d *DB) updateIndex(realKey []byte, recordInfo *data.RecordInfo, pos *data.RecordPos) {
	if recordInfo.Type == data.DeleteRecord {
		d.index.Delete(realKey)
	} else {
		d.index.Put(realKey, pos)
	}
}

// 加载数据文件
func (d *DB) loadDataFiles() error {
	// 通过配置项把目录读取出来
	dirEntries, err := os.ReadDir(d.options.DirPath)
	if err != nil {
		log.Error().Msgf("ReadDir error,err = %v", err)
		return err
	}

	var fileIDs []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), ".data") {
			fileNameList := strings.Split(entry.Name(), ".")
			fileID, err := strconv.Atoi(fileNameList[0])
			if err != nil {
				log.Error().Msgf("Atoi error,err = %v", err)
				return err
			}
			fileIDs = append(fileIDs, fileID)
		}
	}

	sort.Ints(fileIDs)

	// 遍历文件id 获取文件句柄
	for i, fileID := range fileIDs {
		dataFile, err := data.NewDataFile(d.options.DirPath, fileID)
		if err != nil {
			log.Error().Msgf("OpenDataFile error,err = %v", err)
			return err
		}
		if i == len(fileIDs)-1 {
			// 最后一个文件设置为活跃文件
			d.activeFile = dataFile
		} else {
			d.fileMapping[fileID] = dataFile
		}
	}

	d.fileIDs = fileIDs

	return nil
}

// Put 向db中写入数据
func (d *DB) Put(key []byte, value []byte) error {
	if key == nil {
		return ErrKeyIsNil
	}

	record := data.RecordInfo{
		Key:   encodeKeyWithSeqNo(key, noTrxSeqNo),
		Value: value,
		Type:  1,
	}

	pos, err := d.appendRecord(&record)

	if err != nil {
		log.Error().Msgf("appendRecord error,err = %v", err)
		return err
	}

	// 更新内存索引
	if err := d.index.Put(key, pos); err != nil {
		log.Error().Msgf("index Put error,err = %v", err)
		return err
	}
	return nil
}

func (d *DB) appendRecord(record *data.RecordInfo) (*data.RecordPos, error) {
	// 向文件中写入记录 并返回索引 将索引保存在内存中
	d.lock.Lock()
	d.lock.Unlock()

	if d.activeFile == nil {
		// 当前的活跃文件为空 则设置活跃文件
		if err := d.setActiveFile(); err != nil {
			log.Error().Msgf("setActiveFile error,err = %v", err)
			return nil, err
		}
	}

	enRecord, size := data.EncodeRecord(record)

	if d.activeFile.WriteOffSet+size > d.options.DataFileSize {
		if err := d.activeFile.Sync(); err != nil {
			log.Error().Msgf("appendRecord error,err = %v", err)
			return nil, err
		}

		d.fileMapping[d.activeFile.FileID] = d.activeFile

		if err := d.setActiveFile(); err != nil {
			log.Error().Msgf("appendRecord error,err = %v", err)
			return nil, err
		}
	}

	writeOffset := d.activeFile.WriteOffSet

	d.activeFile.Write(enRecord)

	if d.options.SyncWrites {
		if err := d.activeFile.Sync(); err != nil {
			log.Error().Msgf("appendRecord error,err = %v", err)
			return nil, err
		}
	}

	return &data.RecordPos{
		FileID: d.activeFile.FileID,
		Offset: writeOffset,
	}, nil
}

// 设置当前的活跃文件
func (d *DB) setActiveFile() error {
	fileID := 0
	if d.activeFile != nil {
		fileID = d.activeFile.FileID + 1
	}

	dataFile, err := data.NewDataFile(d.options.DirPath, fileID)
	if err != nil {
		log.Error().Msgf("NewDataFile error,err = %v", err)
		return err
	}
	d.activeFile = dataFile
	return nil
}

// Get 从db中获取数据
func (d *DB) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrKeyIsNil
	}

	pos, err := d.index.Get(key)
	if err != nil {
		return nil, err
	}

	recordInfo, err := d.getValueByPos(pos)
	if err != nil {
		log.Error().Msgf("getValueByPos error,err = %v", err)
		return nil, err
	}

	return recordInfo.Value, nil
}

func (d *DB) getValueByPos(pos *data.RecordPos) (*data.RecordInfo, error) {
	// 在文件中从对应的位置获取数据
	if pos == nil {
		// 说明key不存在
		return nil, nil
	}

	/*
		根据文件id找到对应的数据文件
		从数据文件中获取数据
	*/

	var dataFile *data.DataFile

	if d.activeFile.FileID == pos.FileID {
		dataFile = d.activeFile
	} else {
		dataFile = d.fileMapping[pos.FileID]
	}

	if dataFile == nil {
		return nil, errors.New("dataFile is nil")
	}

	recordInfo, _, err := dataFile.ReadRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if recordInfo.Type == 2 {
		log.Log().Msgf("key is deleted,fileID = %v,offSet = %v", pos.FileID, pos.Offset)
		return nil, nil
	}

	return recordInfo, nil
}

func (d *DB) Delete(key []byte) error {
	if key == nil {
		return ErrKeyIsNil
	}

	// 从内存索引中查找key
	pos, err := d.index.Get(key)
	if err != nil {
		log.Error().Msgf("index Get error,err = %v", err)
		return err
	}

	if pos == nil {
		return nil
	}

	record := data.RecordInfo{
		Key:  key,
		Type: 2,
	}

	if _, err := d.appendRecord(&record); err != nil {
		log.Error().Msgf("appendRecord error,err = %v", err)
		return err
	}

	return d.index.Delete(key)
}

// Close 关闭数据库
func (d *DB) Close() error {
	if d.activeFile == nil {
		return nil
	}

	d.lock.Lock()
	defer d.lock.Unlock()

	// 保存当前事务序列号
	seqFile, err := data.NewSeqNumFile(d.options.DirPath)
	if err != nil {
		log.Error().Msgf("NewSeqNumFile error,err = %v", err)
		return err
	}

	seqRecord := &data.RecordInfo{
		Key:   []byte{},
		Value: []byte(strconv.Itoa(int(d.seqNo))),
	}

	dataSeqRecord, _ := data.EncodeRecord(seqRecord)

	if err := seqFile.Write(dataSeqRecord); err != nil {
		log.Error().Msgf("Write error,err = %v", err)
		return err
	}

	if err := seqFile.Sync(); err != nil {
		log.Error().Msgf("Write error,err = %v", err)
		return err
	}

	// 关闭当前活跃文件
	if err := d.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧数据文件
	for _, file := range d.fileMapping {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// ListKeys 获取数据库中所有的key
func (d *DB) ListKeys() [][]byte {
	it := d.index.Iterator(false)

	keys := make([][]byte, 0)

	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, it.Key())
	}

	return keys
}

// Fold 获取的数据 并执行用户指定的操作
func (d *DB) Fold(fn func(key []byte, value []byte) bool) error {
	if fn == nil {
		return errors.New("fn is nil")
	}
	it := d.index.Iterator(false)
	for it.Rewind(); it.Valid(); it.Next() {
		value, err := d.getValueByPos(it.Value())
		if err != nil {
			return err
		}

		if !fn(it.Key(), value.Value) {
			break
		}
	}

	return nil
}

// Sync 刷盘
func (d *DB) Sync() error {
	if d.activeFile == nil {
		return nil
	}

	return d.activeFile.Sync()
}

func (d *DB) loadSeqNum() error {
	d.isSeqFileExist = true
	return nil
}
