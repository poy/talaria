package files

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/apoydence/talaria/logging"
)

type SegmentedFileReader struct {
	dir         string
	file        *os.File
	currentFile int
	pollTime    time.Duration
	log         logging.Logger
}

func NewSegmentedFileReader(dir string, pollTime time.Duration) *SegmentedFileReader {
	return &SegmentedFileReader{
		dir:         dir,
		currentFile: -1,
		pollTime:    pollTime,
		log:         logging.Log("SegmentedFileReader"),
	}
}

func (s *SegmentedFileReader) Read(buffer []byte) (int, error) {
	file := s.fetchFile()

	n, err := file.Read(buffer)
	if err == io.EOF {
		s.file = nil
		return s.Read(buffer)
	}
	return n, err
}

func (s *SegmentedFileReader) fetchFile() *os.File {
	for {
		if s.file != nil {
			return s.file
		}

		file := s.openFile(s.currentFile + 1)
		if file != nil {
			return file
		}

		next, ok := s.fetchNextNumber()
		if ok {
			file = s.openFile(next)
			if file != nil {
				return file
			}
		}

		time.Sleep(s.pollTime)
	}

}

func (s *SegmentedFileReader) openFile(number int) *os.File {
	file, err := os.Open(path.Join(s.dir, fmt.Sprintf("%d", number)))
	if err != nil {
		return nil
	}
	s.currentFile = number
	return file
}

func (s *SegmentedFileReader) fetchNextNumber() (int, bool) {
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

	for _, i := range fileNames {
		if i > s.currentFile {
			return i, true
		}
	}

	return -1, false
}
