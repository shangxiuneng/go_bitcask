package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(path string) {
	if err := os.Remove(path); err != nil {
		panic(err)
	}
}

// TODO 需要新建一个文件 temp
func TestNewFileIO(t *testing.T) {
	path := filepath.Join("temp", "a.data")
	fio, err := NewFileIO(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("temp", "a.data")
	fio, err := NewFileIO(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("hello world"))
	assert.Equal(t, 11, n)
	assert.Nil(t, err)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("temp", "a.data")
	fio, err := NewFileIO(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello world"))
	assert.Equal(t, 11, n)
	assert.Nil(t, err)
	//
	got := make([]byte, 11)
	n, err = fio.Read(got, 0)
	assert.Nil(t, err)
	assert.Equal(t, 11, n)
	assert.Equal(t, []byte("hello world"), got)

	err = fio.Close()
	assert.Nil(t, err)
}
