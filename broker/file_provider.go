package broker

import (
	"os"
	"path"
	"sync"
	"time"

	"github.com/apoydence/talaria/files"
	"github.com/apoydence/talaria/logging"
)

type FileProvider struct {
	log           logging.Logger
	dir           string
	desiredLength uint64
	maxSegments   uint64
	polling       time.Duration

	lock      sync.RWMutex
	writerMap map[string]InitableWriter
}

func NewFileProvider(dir string, desiredLength, maxSegments uint64, polling time.Duration) *FileProvider {
	return &FileProvider{
		log:           logging.Log("FileProvider"),
		dir:           dir,
		desiredLength: desiredLength,
		maxSegments:   maxSegments,
		writerMap:     make(map[string]InitableWriter),
		polling:       polling,
	}
}

func (f *FileProvider) ProvideWriter(name string) InitableWriter {
	f.lock.Lock()
	defer f.lock.Unlock()

	writer, ok := f.writerMap[name]
	if !ok {
		dir := path.Join(f.dir, name)
		segWriter := files.NewSegmentedFileWriter(dir, f.desiredLength, f.maxSegments)
		f.writerMap[name] = segWriter
		writer = segWriter
	}

	return writer
}

func (f *FileProvider) ProvideReader(name string) IndexReader {
	return f.provideReader(name, f.polling)
}

func (f *FileProvider) ProvideLastIndex(name string) (uint64, bool) {
	dir := path.Join(f.dir, name)
	if !exists(dir) {
		return 0, false
	}

	segWriter := files.NewSegmentedFileWriter(dir, f.desiredLength, f.maxSegments)
	return segWriter.LastIndex(), true
}

func (f *FileProvider) provideReader(name string, polling time.Duration) *files.SegmentedFileReader {
	return files.NewSegmentedFileReader(path.Join(f.dir, name), polling)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}

	if os.IsNotExist(err) {
		return false
	}

	return true
}
