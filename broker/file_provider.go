package broker

import (
	"io"
	"path"
	"sync"
	"time"

	"github.com/apoydence/talaria/files"
	"github.com/apoydence/talaria/logging"
)

type writerInfo struct {
	writer  io.Writer
	replica uint
}

type FileProvider struct {
	log           logging.Logger
	dir           string
	desiredLength uint64
	maxSegments   uint64
	polling       time.Duration

	lock      sync.RWMutex
	writerMap map[string]*writerInfo
}

func NewFileProvider(dir string, desiredLength, maxSegments uint64, polling time.Duration) *FileProvider {
	return &FileProvider{
		log:           logging.Log("FileProvider"),
		dir:           dir,
		desiredLength: desiredLength,
		maxSegments:   maxSegments,
		writerMap:     make(map[string]*writerInfo),
		polling:       polling,
	}
}

func (f *FileProvider) ProvideWriter(name string, replica uint) io.Writer {
	f.lock.Lock()
	defer f.lock.Unlock()

	info, ok := f.writerMap[name]
	if !ok {
		info = &writerInfo{
			writer:  files.NewChunkedFileWriter(files.NewSegmentedFileWriter(path.Join(f.dir, name), f.desiredLength, f.maxSegments)),
			replica: replica,
		}
		f.writerMap[name] = info
	}

	if info.replica != replica {
		return nil
	}

	return info.writer
}

func (f *FileProvider) ProvideReader(name string) io.Reader {
	return files.NewChunkedFileReader(files.NewSegmentedFileReader(path.Join(f.dir, name), f.polling))
}

func (f *FileProvider) Participate(name string, index uint) bool {
	_, ok := f.writerMap[name]
	return !ok
}
