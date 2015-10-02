package orchestrator

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/apoydence/talaria/logging"
)

type PartitionManager interface {
	Participate(name string, index uint) bool
	ProvideWriter(name string, replica uint) io.Writer
}

type KvStore interface {
	Announce(name string)
	Acquire(name string) bool
	ListenForAnnouncements(callback func(name string))
	ListenForLeader(name string, callback func(name, uri string))
	FetchLeader(name string) (string, bool)
}

type Orchestrator struct {
	log              logging.Logger
	partManager      PartitionManager
	kvStore          KvStore
	clientAddr       string
	numberOfReplicas uint
}

func New(clientAddr string, numberOfReplicas uint, partManager PartitionManager, kvStore KvStore) *Orchestrator {
	orch := &Orchestrator{
		log:              logging.Log("Orchestrator"),
		partManager:      partManager,
		kvStore:          kvStore,
		clientAddr:       clientAddr,
		numberOfReplicas: numberOfReplicas,
	}

	kvStore.ListenForAnnouncements(orch.participateInElection)

	return orch
}

func (o *Orchestrator) FetchLeader(name string) (string, bool) {
	uri, ok := o.kvStore.FetchLeader(name)
	if ok {
		return uri, o.clientAddr == uri
	}

	results := make(chan string, 1)

	o.kvStore.ListenForLeader(name, func(name, uri string) {
		results <- uri
	})

	o.kvStore.Announce(name + "~0")

	result := <-results
	return result, result == o.clientAddr
}

func (o *Orchestrator) participateInElection(fullName string) {
	name, replica := o.decodeIndex(fullName)
	if !o.partManager.Participate(name, replica) {
		return
	}

	acquired := o.kvStore.Acquire(name)
	if !acquired {
		return
	}

	o.partManager.ProvideWriter(name, replica)

	if replica < o.numberOfReplicas {
		o.kvStore.Announce(o.encodeIndex(name, replica+1))
	}
}

func (o *Orchestrator) encodeIndex(name string, replica uint) string {
	return fmt.Sprintf("%s~%d", name, replica)
}

func (o *Orchestrator) decodeIndex(name string) (string, uint) {
	results := strings.Split(name, "~")
	if len(results) != 2 {
		o.log.Panicf("%s has not been properly encoded with the replica index", name)
	}

	index, err := strconv.Atoi(results[1])
	if err != nil {
		o.log.Panicf("%s has not been properly encoded with the replica index: %v", name, err)
	}

	return results[0], uint(index)
}
