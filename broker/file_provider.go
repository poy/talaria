package broker

import (
	"io"
	"path"
	"sync"
	"time"

	"github.com/apoydence/talaria/files"
)

type FileProvider struct {
	dir           string
	desiredLength uint64
	maxSegments   uint64
	polling       time.Duration

	lock      sync.RWMutex
	writerMap map[string]io.Writer
}

func NewFileProvider(dir string, desiredLength, maxSegments uint64, polling time.Duration) *FileProvider {
	return &FileProvider{
		dir:           dir,
		desiredLength: desiredLength,
		maxSegments:   maxSegments,
		writerMap:     make(map[string]io.Writer),
		polling:       polling,
	}
}

func (f *FileProvider) ProvideWriter(name string) io.Writer {
	f.lock.Lock()
	defer f.lock.Unlock()

	writer, ok := f.writerMap[name]
	if !ok {
		writer = files.NewSegmentedFileWriter(path.Join(f.dir, name), f.desiredLength, f.maxSegments)
		f.writerMap[name] = writer
	}
	return writer
}

func (f *FileProvider) ProvideReader(name string) io.Reader {
	return files.NewSegmentedFileReader(path.Join(f.dir, name), f.polling)
}
