package fileutil

import (
	"io"
	"os"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/require"
)

func TestAlignedSize(t *testing.T) {
	tests := []struct {
		size          int64
		alignedBlocks int64
	}{
		{0, 0},
		{1, 1},
		{4095, 1},
		{4096, 1},
		{4097, 2},
		{8191, 2},
		{8192, 2},
		{8193, 3},
	}
	for _, tt := range tests {
		if alignedSize(tt.size) != tt.alignedBlocks*directio.BlockSize {
			t.FailNow()
		}
	}
}

func TestDirectWriter(t *testing.T) {
	fileName := "direct_test"
	fd, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	directFile := NewDirectWriter(fd, directio.BlockSize, nil)
	defer os.Remove(fileName)
	require.Nil(t, err)
	val := make([]byte, 1000)
	for i := 0; i < 100; i++ {
		setVal(val, byte(i))
		err := directFile.Append(val)
		require.Nil(t, err)
	}
	err = directFile.Finish()
	require.Nil(t, err)
	fd.Close()
	file, err := os.Open(fileName)
	require.Nil(t, err)
	info, err := file.Stat()
	require.Nil(t, err)
	require.Equal(t, info.Size(), int64(100*1000))
	for i := 0; i < 100; i++ {
		_, err = io.ReadFull(file, val)
		require.Nil(t, err)
		for _, v := range val {
			require.Equal(t, v, byte(i))
		}
	}
	file.Close()
}

func setVal(buf []byte, v byte) {
	for i := range buf {
		buf[i] = v
	}
}
