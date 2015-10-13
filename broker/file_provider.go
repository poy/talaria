package broker

import (
	"io"
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
	writerMap map[string]SubscribableWriter
}

func NewFileProvider(dir string, desiredLength, maxSegments uint64, polling time.Duration) *FileProvider {
	return &FileProvider{
		log:           logging.Log("FileProvider"),
		dir:           dir,
		desiredLength: desiredLength,
		maxSegments:   maxSegments,
		writerMap:     make(map[string]SubscribableWriter),
		polling:       polling,
	}
}

func (f *FileProvider) ProvideWriter(name string) SubscribableWriter {
	f.lock.Lock()
	defer f.lock.Unlock()

	writer, ok := f.writerMap[name]
	if !ok {
		segWriter := files.NewSegmentedFileWriter(path.Join(f.dir, name), f.desiredLength, f.maxSegments)
		chunkedWriter := files.NewChunkedFileWriter(segWriter)
		writer = files.NewReplicatedFileLeader(chunkedWriter, nil)
		f.writerMap[name] = writer
	}

	return writer
}

func (f *FileProvider) ProvideReader(name string) io.Reader {
	return files.NewChunkedFileReader(files.NewSegmentedFileReader(path.Join(f.dir, name), f.polling))
}
