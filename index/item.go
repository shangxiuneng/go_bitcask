package index

import (
	"bytes"
	"github.com/google/btree"
	"go_bitcask/data"
)

type Item struct {
	key    []byte
	record *data.RecordPos
}

func (i *Item) Less(item btree.Item) bool {
	return bytes.Compare(i.key, item.(*Item).key) == -1
}
