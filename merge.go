package go_bitcask

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go_bitcask/data"
	"io"
)

var (
	mergeFile = ""
)

func (d *DB) Merge() error {
	if d.isMerge {
		return errors.New("merging")
	}

	d.isMerge = true
	defer func() {
		d.isMerge = false
	}()

	// 持久化当前文件
	if err := d.activeFile.Sync(); err != nil {
		log.Error().Msgf("Sync error,err = %v", err)
		return err
	}

	d.fileMapping[d.activeFile.FileID] = d.activeFile

	if err := d.setActiveFile(); err != nil {
		log.Error().Msgf("setActiveFile error,err = %v", err)
		return err
	}

	// 没有merge的文件id
	noMergeFileID := d.activeFile.FileID

	// 获取所有待merge的文件
	mergeFiles := make([]*data.DataFile, 0)
	for _, v := range d.fileMapping {
		mergeFiles = append(mergeFiles, v)
	}

	// 获取merge文件的路径
	// 如果文件存在 则存在merge文件 删除掉 重新尽力一个文件

	// 新建一个bitcask实例
	mergeDB, err := Open(Config{
		DirPath: "",
	})

	if err != nil {
		log.Error().Msgf("Open error,err = %v", err)
		return err
	}

	hintFile, err := data.NewHintFile(mergePath)
	if err != nil {
		log.Error().Msgf("NewHintFile error,err = %v", err)
		return err
	}
	for _, dataFile := range mergeFiles {
		offset := 0
		for {
			recordInfo, size, err := dataFile.ReadRecord(offset)
			if err != nil {
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
			}

			realKey, _ := parseKeyWithSeqNo(recordInfo.Key)
			posInfo, err := d.index.Get(realKey)
			if err != nil {
				return err
			}
			if posInfo != nil &&
				posInfo.FileID == dataFile.FileID &&
				posInfo.Offset == offset {
				// 重写当前记录

				recordInfo.Key = encodeKeyWithSeqNo(realKey, noTrxSeqNo)
				posInfo, err := mergeDB.appendRecord(recordInfo)
				if err != nil {
					return err
				}

				// 将当前的索引信息写入到文件中
				if err := hintFile.WriteHintFile(realKey, posInfo); err != nil {
					log.Error().Msgf("WriteHintFile error,err = %v", err)
					return err
				}
			}

			offset = offset + size
		}
	}

	if err := mergeDB.Sync(); err != nil {
		log.Error().Msgf("Sync error,err = %v", err)
		return err
	}

	return nil
}

func (d *DB) getMergePath() {

}
