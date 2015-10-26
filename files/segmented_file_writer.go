package files

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/pb/filemeta"
	"github.com/gogo/protobuf/proto"
)

type SegmentedFileWriter struct {
	lock          sync.Mutex
	dir           string
	file          *os.File
	chunkedWriter *ChunkedFileWriter
	desiredLength uint64
	maxSegments   int
	currentLength uint64
	nextIndex     int
	log           logging.Logger
	lengthBuffer  []byte

	startingIndex uint64
	writeCount    uint64
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
		lengthBuffer:  make([]byte, 8),
	}
}

func (s *SegmentedFileWriter) Write(data []byte) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.currentLength >= s.desiredLength {
		s.closeFile(s.file)
		s.createNewFile()
		s.rollSegments()
	}

	s.writeCount++
	s.currentLength += uint64(len(data)) + 8
	return s.chunkedWriter.Write(data)
}

func (s *SegmentedFileWriter) rollSegments() {
	if s.nextIndex > s.maxSegments {
		deletePath := path.Join(s.dir, fmt.Sprintf("%d", s.nextIndex-1-s.maxSegments))
		s.log.Debug("Deleting file %s", deletePath)
		err := os.Remove(deletePath)
		if err != nil {
			s.log.Panic("Failed to delete file", err)
		}
	}
}

func (s *SegmentedFileWriter) createNewFile() {
	var err error
	path := path.Join(s.dir, fmt.Sprintf("%d", s.nextIndex))
	s.log.Debug("Creating new file %s", path)
	s.file, err = os.Create(path)
	if err != nil {
		s.log.Panic("Failed to create file", err)
	}

	if _, err = s.file.Seek(8, 0); err != nil {
		s.log.Panic("Failed to seek within file", err)
	}

	s.chunkedWriter = NewChunkedFileWriter(s.file)

	s.nextIndex++
	s.currentLength = 0
	s.startingIndex = s.writeCount
}

func (s *SegmentedFileWriter) writeMeta() {
	s.log.Debug("Writing meta for %s", s.file.Name())

	if _, err := s.file.Seek(0, 0); err != nil {
		s.log.Panic("Failed to seek within file", err)
	}

	binary.LittleEndian.PutUint64(s.lengthBuffer, s.currentLength+8)
	if _, err := s.file.Write(s.lengthBuffer); err != nil {
		s.log.Panic("Failed to write current length for meta", err)
	}

	if _, err := s.file.Seek(0, 2); err != nil {
		s.log.Panic("Failed to seek to the end of the file", err)
	}

	meta := &filemeta.FileMeta{
		StartingIndex: proto.Uint64(s.startingIndex),
		Count:         proto.Uint64(s.writeCount - s.startingIndex),
	}

	metaData, err := meta.Marshal()
	if err != nil {
		s.log.Panic("Failed to marshal meta data", err)
	}

	if _, err = s.file.Write(metaData); err != nil {
		s.log.Panic("Failed to write meta data", err)
	}
}

func (s *SegmentedFileWriter) closeFile(file *os.File) {
	if file == nil {
		return
	}

	s.writeMeta()

	err := file.Sync()
	if err != nil {
		s.log.Panic("Failed to sync file", err)
	}

	err = file.Close()
	if err != nil {
		s.log.Panic("Failed to close file", err)
	}
}
