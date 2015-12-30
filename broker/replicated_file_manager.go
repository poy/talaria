package broker

import (
	"io"
	"sync"

	"github.com/apoydence/talaria/logging"
)

type ReaderFetcher interface {
	FetchReader(name string) (*Reader, error)
}

type writerInfo struct {
	writer  io.Writer
	replica uint
}

type ReplicatedFileManager struct {
	log           logging.Logger
	ioProvider    IoProvider
	readerFetcher ReaderFetcher

	lockWriters sync.RWMutex
	writers     map[string]*writerInfo
}

func NewReplicatedFileManager(ioProvider IoProvider, readerFetcher ReaderFetcher) *ReplicatedFileManager {
	return &ReplicatedFileManager{
		log:           logging.Log("ReplicatedFileManager"),
		ioProvider:    ioProvider,
		readerFetcher: readerFetcher,
		writers:       make(map[string]*writerInfo),
	}
}

func (r *ReplicatedFileManager) Add(name string, replica uint) (uint, bool) {
	r.lockWriters.Lock()
	defer r.lockWriters.Unlock()

	currentReplica, currentOk := r.currentReplica(name)

	if replica == 0 {
		r.writers[name] = &writerInfo{
			replica: 0,
		}
		return currentReplica, currentOk
	}

	r.log.Debug("Adding replica %d for %s", replica, name)
	writer := r.ioProvider.ProvideWriter(name)
	reader, err := r.readerFetcher.FetchReader(name)
	if err != nil {
		r.log.Panic("Unable to fetch reader", err)
	}

	r.writers[name] = &writerInfo{
		writer:  writer,
		replica: replica,
	}

	go r.copyToWriter(reader, writer)

	return currentReplica, currentOk
}

func (r *ReplicatedFileManager) Participate(name string, replica uint) bool {
	r.lockWriters.RLock()
	defer r.lockWriters.RUnlock()

	info, ok := r.writers[name]
	if !ok || info.replica > replica {
		return true
	}

	return false
}

func (r *ReplicatedFileManager) currentReplica(name string) (uint, bool) {
	info, ok := r.writers[name]
	if !ok {
		return 0, false
	}

	return info.replica, true
}

func (r *ReplicatedFileManager) copyToWriter(reader *Reader, writer InitableWriter) {
	var currentIndex int64
	for {
		data, index, err := reader.ReadFromFile()
		if err != nil {
			r.log.Error("Unable to read data,", err)
			return
		}

		prevIndex := currentIndex
		currentIndex = index
		if r.initWriter(prevIndex, index, data, writer, reader) {
			continue
		}

		if n, err := writer.Write(data); err != nil || n != len(data) {
			r.log.Panicf("Unable to write data (len = %d, n = %d) (err = %v)", len(data), n, err)
		}
		currentIndex++
	}
}

func (r *ReplicatedFileManager) initWriter(currentIndex, index int64, data []byte, writer InitableWriter, reader *Reader) bool {
	if currentIndex == index {
		return false
	}

	r.log.Debug("Not replicating from beginning of file (index=%d, currentIndex=%d).", index, currentIndex)
	writerIndex, err := writer.InitWriteIndex(index, data)
	if err != nil {
		r.log.Panic("Unable to write init data", err)
	}

	if writerIndex == index {
		return true
	}

	if err := reader.SeekIndex(uint64(writerIndex)); err != nil {
		r.log.Panicf("Unable to seek to index %d: %v", writerIndex, err)
	}

	return true
}
