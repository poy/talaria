package broker

import (
	"io"
	"sync"
)

type InnerBrokerProvider interface {
	ProvideConn(name, addr string) io.Writer
}

type ReplicaListener interface {
	ListenForReplicas(name string, callback func(name string, replica uint, addr string))
}

type WriterFactory interface {
	ProvideWriter(name string) io.Writer
}

type writerInfo struct {
	writer  io.Writer
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
		//innerConn := r.innerBrokerProvider.ProvideConn(name, addr)
		println("TODO: START READING")
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
