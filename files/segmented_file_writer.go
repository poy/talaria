package files

import (
	"fmt"
	"os"
	"path"

	"github.com/apoydence/talaria/logging"
)

type SegmentedFileWriter struct {
	dir           string
	file          *os.File
	desiredLength uint64
	maxSegments   int
	currentLength uint64
	nextIndex     int
	log           logging.Logger
}

func NewSegmentedFileWriter(dir string, desiredLength, maxSegments uint64) *SegmentedFileWriter {
	log := logging.Log("SegmentedFileWriter")
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		log.Panic("Failed to create directory", err)
	}
	log.Debug("Created directory %s", dir)

	return &SegmentedFileWriter{
		dir:           dir,
		desiredLength: desiredLength,
		currentLength: desiredLength + 1,
		maxSegments:   int(maxSegments),
		log:           log,
	}
}

func (s *SegmentedFileWriter) Write(data []byte) (int, error) {
	var err error
	if s.currentLength >= s.desiredLength {
		s.closeFile(s.file)
		s.file, err = os.Create(path.Join(s.dir, fmt.Sprintf("%d", s.nextIndex)))
		if err != nil {
			s.log.Panic("Failed to create file", err)
		}
		s.nextIndex++
		s.currentLength = 0
		s.rollSegments()
	}

	s.currentLength += uint64(len(data))
	return s.file.Write(data)
}

func (s *SegmentedFileWriter) rollSegments() {
	if s.nextIndex > s.maxSegments {
		deletePath := path.Join(s.dir, fmt.Sprintf("%d", s.nextIndex-1-s.maxSegments))
		err := os.Remove(deletePath)
		if err != nil {
			s.log.Panic("Failed to delete file", err)
		}
	}
}

func (s *SegmentedFileWriter) closeFile(file *os.File) {
	if file == nil {
		return
	}

	err := file.Sync()
	if err != nil {
		s.log.Panic("Failed to sync file", err)
	}

	err = file.Close()
	if err != nil {
		s.log.Panic("Failed to close file", err)
	}
}
