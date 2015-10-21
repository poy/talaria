package files

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/pb/filemeta"
)

type SegmentedFileReader struct {
	dir           string
	file          *os.File
	lastOffset    int64
	currentIndex  uint64
	metaStart     uint64
	currentFile   int
	pollTime      time.Duration
	log           logging.Logger
	lengthBuffer  []byte
	metaBuffer    []byte
	chunkedReader *ChunkedFileReader
}

func NewSegmentedFileReader(dir string, pollTime time.Duration) *SegmentedFileReader {
	return &SegmentedFileReader{
		dir:          dir,
		currentFile:  -1,
		pollTime:     pollTime,
		log:          logging.Log("SegmentedFileReader"),
		lengthBuffer: make([]byte, 8),
		metaBuffer:   make([]byte, 1024),
	}
}

func (s *SegmentedFileReader) Read(buffer []byte) (int, error) {
	s.fetchFile()

	n, err := s.chunkedReader.Read(buffer)
	if n > 0 {
		s.lastOffset += int64(n) + 8
	}

	if err == io.EOF && s.pollTime > 0 {
		s.file.Close()
		s.file = nil
		return s.Read(buffer)
	}
	return s.removeMeta(n), err
}

func (s *SegmentedFileReader) readMeta(file *os.File, metaStart int64) *filemeta.FileMeta {
	index, err := file.Seek(metaStart, 0)
	if err != nil || index != metaStart {
		s.log.Error("Unable to seek in file", err)
		return nil
	}

	n, err := file.Read(s.metaBuffer)
	if err != io.EOF && err != nil {
		s.log.Error("Unable to read meta from file", err)
		return nil
	}

	var meta filemeta.FileMeta
	err = meta.Unmarshal(s.metaBuffer[:n])
	if err != nil {
		s.log.Error("Unable to unmarshal meta data", err)
		return nil
	}

	return &meta
}

func (s *SegmentedFileReader) removeMeta(n int) int {
	if uint64(s.lastOffset+8) < s.metaStart || s.metaStart == 0 {
		return n
	}
	return n - int(uint64(s.lastOffset+8)-s.metaStart)
}

func (s *SegmentedFileReader) fetchFile() {
	for {
		if s.file != nil {
			return
		}

		file, metaStart := s.openFile(s.currentFile + 1)
		if file != nil {
			s.lastOffset = 0
			s.metaStart = metaStart
			s.chunkedReader = NewChunkedFileReader(file)
			return
		}

		next, ok := s.fetchNextNumber(true)
		if ok {
			file, metaStart = s.openFile(next)
			if file != nil {
				s.lastOffset = 0
				s.metaStart = metaStart
				s.file = file
				s.chunkedReader = NewChunkedFileReader(file)
				return
			}
		}

		time.Sleep(s.pollTime)

		file, metaStart = s.openFile(s.currentFile)
		if file != nil {
			_, err := file.Seek(8+s.lastOffset, 0)
			if err != nil {
				s.log.Panic("Unable to set offset", err)
			}
			s.metaStart = metaStart
			s.file = file
			s.chunkedReader = NewChunkedFileReader(file)
			return
		}
	}
}

func (s *SegmentedFileReader) openFile(number int) (*os.File, uint64) {
	filePath := path.Join(s.dir, fmt.Sprintf("%d", number))
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0
	}

	n, err := file.Read(s.lengthBuffer)

	if err == io.EOF {
		if n != 8 {
			return nil, 0
		}
		err = nil
	}

	if err != nil || n != 8 {
		s.log.Panicf("Unable to read meta index from file: (n=%d) (err=%v)", n, err)
	}

	metaStart := binary.LittleEndian.Uint64(s.lengthBuffer)

	s.currentFile = number
	return file, metaStart
}

func (s *SegmentedFileReader) fetchNextNumber(last bool) (int, bool) {
	files, err := ioutil.ReadDir(s.dir)
	if err != nil {
		s.log.Panic("Unable to read directory", err)
	}

	fileNames := make([]int, 0)
	for _, f := range files {
		fn, err := strconv.Atoi(f.Name())
		if err == nil {
			fileNames = append(fileNames, fn)
		}
	}

	sort.Ints(fileNames)

	if !last && len(fileNames) > 0 {
		return fileNames[0], true
	}

	for _, i := range fileNames {
		if i > s.currentFile {
			return i, true
		}
	}

	return -1, false
}
