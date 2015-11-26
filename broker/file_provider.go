package broker

import (
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
		dir := path.Join(f.dir, name)
		segWriter := files.NewSegmentedFileWriter(dir, f.desiredLength, f.maxSegments)
		preReader := newTempSeekWrapper(f.provideReader(name, 0))
		writer = files.NewReplicatedFileLeader(segWriter, preReader)
		f.writerMap[name] = writer
	}

	return writer
}

func (f *FileProvider) ProvideReader(name string) OffsetReader {
	return f.provideReader(name, f.polling)
}

func (f *FileProvider) provideReader(name string, polling time.Duration) OffsetReader {
	return files.NewSegmentedFileReader(path.Join(f.dir, name), polling)
}

type tempSeekWrapper struct {
	OffsetReader
}

func newTempSeekWrapper(reader OffsetReader) *tempSeekWrapper {
	return &tempSeekWrapper{
		OffsetReader: reader,
	}
}

func (t *tempSeekWrapper) Seek(offset int64, relative int) (int64, error) {
	println("TODO tempSeekWrapper")
	return offset, nil
}
