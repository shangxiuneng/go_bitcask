package go_bitcask

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go_bitcask/data"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

var (
	MergeFileName = ""
	MergeFinKey   = ""
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
	mergePath := d.getMergePath()
	if _, err := os.Stat(mergePath); err != nil {
		if err := os.RemoveAll(mergePath); err != nil {
			log.Error().Msgf("RemoveAll error,err = %v", err)
			return err
		}
	}

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

	// 写标识merge完成的文件
	mergeFile, err := data.NewMergeFinFile(mergePath)
	if err != nil {
		log.Error().Msgf("NewMergeFinFile error,err = %v", err)
		return err
	}

	mergeFinRecord := &data.RecordInfo{
		Key:   []byte(MergeFinKey),
		Value: []byte(strconv.Itoa(noMergeFileID)),
	}

	dataMergeFin, _ := data.EncodeRecord(mergeFinRecord)

	if err := mergeFile.Write(dataMergeFin); err != nil {
		return err
	}

	return nil
}

func (d *DB) getMergePath() string {
	return ""
}

func (d *DB) loadMergeFile() error {
	mergePath := d.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)

	if err != nil {
		log.Error().Msgf("ReadDir error,err = %v", err)
		return err
	}

	// 查看merge文件是否存在 merge是否完成
	isMergeFin := false
	var mergeFiles []string
	for _, v := range dirEntries {
		if v.Name() == data.MergeFinFileName {
			// merge文件存在 说明merge已经完成
			isMergeFin = true
		}
		mergeFiles = append(mergeFiles, v.Name())
	}

	if !isMergeFin {
		return nil
	}

	// 删除旧的数据文件
	noMergeFileID, err := d.getNoMergeFileID(mergePath)
	if err != nil {
		log.Error().Msgf("getNoMergeFileID error,err = %v", err)
		return err
	}

	for _, v := range d.fileMapping {
		if v.FileID < noMergeFileID {
			// 说明当前的文件已经被merge处理过 可以删除
			filePath := data.GetFileName(mergePath, 0)
			if _, err := os.Stat(filePath); err == nil {
				if err := os.RemoveAll(filePath); err != nil {
					log.Error().Msgf("RemoveAll error,err = %v", err)
					return err
				}
			}
		}
	}

	// 移动新的数据文件
	for _, fileName := range mergeFiles {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(d.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) loadHintFile() error {
	// 查看文件是否存在
	return nil
}

func (d *DB) getNoMergeFileID(mergePath string) (int, error) {
	mergeFinFile, err := data.NewMergeFinFile(mergePath)
	if err != nil {
		return 0, err
	}

	recordInfo, _, err := mergeFinFile.ReadRecord(0)
	if err != nil {
		return 0, err
	}

	fileID, err := strconv.Atoi(string(recordInfo.Key))

	if err != nil {
		return 0, err
	}

	return fileID, nil
}
