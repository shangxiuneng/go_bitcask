package go_bitcask

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go_bitcask/data"
	"go_bitcask/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	lock        *sync.Mutex
	activeFile  *data.DataFile         // 当前获取的文件
	fileMapping map[int]*data.DataFile // 保存文件id到文件句柄的映射
	options     Config                 // 配置项
	index       index.Index            // 内存索引
}

func Open(option Config) (*DB, error) {
	// TODO 校验配置项

	if _, err := os.Stat(option.DirPath); os.IsNotExist(err) {
		// 创建目录
		if err := os.MkdirAll(option.DirPath, os.ModePerm); err != nil {
			log.Error().Msgf("MkdirAll error,err = %v", err)
			return nil, err
		}
	}

	db := &DB{
		options:     option,
		lock:        new(sync.Mutex),
		fileMapping: map[int]*data.DataFile{},
		index:       index.NewIndex(index.BTreeIndex),
	}

	// 加载对应的数据文件
	if err := db.loadDataFiles(); err != nil {
		log.Error().Msgf("loadDataFiles error,err = %v", err)
		return nil, err
	}

	// 构造索引

	return db, nil
}

// TODO 载入索引的过程会非常慢
func (d *DB) loadIndex() error {
	var fileIDs []int

	for i, fileID := range fileIDs {
		dataFile := &data.DataFile{}

		if fileID == d.activeFile.FileID {

		} else {

		}

		offset := 0

		for {
			recordInfo, recordSize, err := dataFile.ReadRecord(offset)
			if err != nil {
				if err == io.EOF {
					// 说明读到文件末尾了
					break
				}
				log.Error().Msgf("ReadRecord error,err = %v", err)
				return err
			}

			pos := data.RecordPos{
				FileID: fileID,
				Offset: offset,
			}

			if recordInfo.RecordType == -1 {
				d.index.Delete(recordInfo.Key)
			} else {
				d.index.Put(recordInfo.Key, &pos)
			}

			offset = offset + recordSize

			if i == len(fileIDs)-1 {
				d.activeFile.WriteOffSet = offset
			}
		}
	}
	return nil
}

func (d *DB) loadDataFiles() error {
	// 通过配置项把目录读取出来
	dirEntries, err := os.ReadDir(d.options.DirPath)
	if err != nil {
		return err
	}
	var fileIDs []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), ".data") {
			fileNameList := strings.Split(entry.Name(), ".")
			fileID, err := strconv.Atoi(fileNameList[0])
			if err != nil {

			}
			fileIDs = append(fileIDs, fileID)
		}
	}

	sort.Ints(fileIDs)

	// 遍历文件id 获取文件句柄

	for i, fileID := range fileIDs {
		dataFile, err := data.OpenDataFile(d.options.DirPath, fileID)
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

	return nil
}

// Put 向db中写入数据
func (d *DB) Put(key []byte, value []byte) error {
	if key == nil {
		return ErrKeyIsNil
	}

	record := data.RecordInfo{
		Key:        key,
		Value:      value,
		RecordType: 1,
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

	if d.options.SyncWrite {
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

	if recordInfo.RecordType == -1 {
		log.Log().Msgf("key is deleted,fileID = %v,offSet = %v", pos.FileID, pos.Offset)
		return nil, nil
	}

	return recordInfo.Value, nil
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
		Key:        key,
		RecordType: -1,
	}

	if _, err := d.appendRecord(&record); err != nil {
		log.Error().Msgf("appendRecord error,err = %v", err)
		return err
	}

	return d.index.Delete(key)
}
