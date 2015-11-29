package broker

import (
	"io"
	"sync"

	"github.com/apoydence/talaria/files"
)

type SubscribableWriter interface {
	io.Writer
	UpdateWriter(writer files.InitableWriter)
	InitWriteIndex(index int64, data []byte) (int64, error)
}

type InnerBrokerProvider interface {
	ProvideConn(name, addr string) files.InitableWriter
}

type ReplicaListener interface {
	ListenForReplicas(name string, callback func(name string, replica uint, addr string))
}

type WriterFactory interface {
	ProvideWriter(name string) SubscribableWriter
}

type writerInfo struct {
	writer  SubscribableWriter
	replica uint
}

type ReplicatedFileManager struct {
	writerFactory       WriterFactory
	innerBrokerProvider InnerBrokerProvider
	replicaListener     ReplicaListener

	lockWriters sync.RWMutex
	writers     map[string]*writerInfo
}

func NewReplicatedFileManager(writerFactory WriterFactory, innerBrokerProvider InnerBrokerProvider, replicaListener ReplicaListener) *ReplicatedFileManager {
	return &ReplicatedFileManager{
		writerFactory:       writerFactory,
		innerBrokerProvider: innerBrokerProvider,
		replicaListener:     replicaListener,
		writers:             make(map[string]*writerInfo),
	}
}

func (r *ReplicatedFileManager) Add(name string, replica uint) {
	writer := r.writerFactory.ProvideWriter(name)

	r.lockWriters.Lock()
	defer r.lockWriters.Unlock()
	r.writers[name] = &writerInfo{
		writer:  writer,
		replica: replica,
	}

	r.replicaListener.ListenForReplicas(name, func(_ string, rep uint, addr string) {
		if replica+1 != rep {
			return
		}
		innerConn := r.innerBrokerProvider.ProvideConn(name, addr)
		writer.UpdateWriter(innerConn)
	})
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
