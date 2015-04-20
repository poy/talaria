package datapipeline

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

var (
	LatestEntry   uint32 = math.MaxUint32
	MoreData             = errors.New("MoreData")
	BlockTooLarge        = errors.New("BlockTooLarge")
)

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

type FileConsumer struct {
	reader       ReadSeekCloser
	lengthBuffer []byte

	partialDataReadLength uint32
	partialDataReadOk     bool
	indexes               []int64
}

func NewFileConsumer(path string, blockIndex uint32) *FileConsumer {
	reader, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	f := &FileConsumer{
		reader:       reader,
		lengthBuffer: make([]byte, 4),
	}
	f.readIndexSlice()
	f.seekBlock(blockIndex)
	return f
}

func (f *FileConsumer) Read(buffer []byte) (int, error) {
	var err error
	length, ok := f.readLength(true)
	bufLen := len(buffer)
	if !ok {
		return -1, io.EOF
	} else if uint32(bufLen) >= length {
		f.partialDataReadOk = false
	} else {
		f.partialDataReadOk = true
		f.partialDataReadLength = length - uint32(bufLen)
		err = MoreData
		length = uint32(bufLen)
	}

	if _, err := f.reader.Read(buffer[:length]); err != nil {
		return -1, err
	}

	return int(length), err
}

func (f *FileConsumer) seekBlock(blockIndex uint32) error {
	if blockIndex == LatestEntry {
		blockIndex = uint32(len(f.indexes)) - 1
	} else if uint32(len(f.indexes)) < blockIndex {
		return io.EOF
	}
	if _, err := f.reader.Seek(f.indexes[blockIndex], 0); err != nil {
		panic(err)
	}
	return nil
}

func (f *FileConsumer) readLength(follow bool) (uint32, bool) {
	if f.partialDataReadOk {
		return f.partialDataReadLength, true
	}

	for {
		if n, err := f.reader.Read(f.lengthBuffer); err != nil && err != io.EOF {
			panic(err)
		} else if err == io.EOF && (!follow || f.isClosed()) {
			return 0, false
		} else if err == io.EOF {
			time.Sleep(1 * time.Second)
		} else if n != 4 {
			panic(fmt.Sprintf("Only read %d bytes for the length. Required 4.", n))
		} else {
			return binary.LittleEndian.Uint32(f.lengthBuffer), true
		}
	}
}

func (f *FileConsumer) isClosed() bool {
	current, err := f.reader.Seek(0, 1)
	if err != nil {
		panic(err)
	}
	defer f.reader.Seek(current, 0)
	_, err = f.reader.Seek(0, 0)
	if err != nil {
		panic(err)
	}

	if n, err := f.reader.Read(f.lengthBuffer); err != nil && err != io.EOF {
		panic(err)
	} else if n != 4 {
		panic(fmt.Sprintf("Only read %d bytes for the length. Required 4.", n))
	}

	return binary.LittleEndian.Uint32(f.lengthBuffer) != 0
}

func (f *FileConsumer) readIndexSlice() {
	f.reader.Seek(4, 0)
	f.indexes = append(f.indexes, 4)
	for {
		length, ok := f.readLength(false)
		if !ok {
			break
		}
		pos, err := f.reader.Seek(int64(length), 1)
		if err != nil {
			panic(err)
		}
		f.indexes = append(f.indexes, pos)
	}
	f.reader.Seek(4, 0)
}
