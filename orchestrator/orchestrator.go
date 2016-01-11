package orchestrator

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apoydence/talaria/logging"
)

type PartitionManager interface {
	Participate(name string, index uint) bool
	Add(name string, replica uint) (uint, bool)
}

type KvStore interface {
	Announce(name string)
	Acquire(name string) bool
	ListenForAnnouncements(callback func(name string))
	ListenForLeader(name string, callback func(name, uri string))
	FetchLeader(name string) (string, bool)
	DeleteAnnouncement(name string)
}

type Orchestrator struct {
	log              logging.Logger
	kvStore          KvStore
	clientAddr       string
	numberOfReplicas uint
}

func New(clientAddr string, numberOfReplicas uint, kvStore KvStore) *Orchestrator {
	orch := &Orchestrator{
		log:              logging.Log("Orchestrator"),
		kvStore:          kvStore,
		clientAddr:       clientAddr,
		numberOfReplicas: numberOfReplicas,
	}

	return orch
}

func (o *Orchestrator) FetchLeader(name string, create bool) (string, bool, error) {
	encodedName := fmt.Sprintf("%s~0", name)

	uri, ok := o.kvStore.FetchLeader(encodedName)
	if ok {
		return uri, o.clientAddr == uri, nil
	}

	if !create {
		return "", false, fmt.Errorf("File (%s) does not exist", name)
	}

	results := make(chan string, 1)

	o.kvStore.ListenForLeader(encodedName, func(name, uri string) {
		results <- uri
	})

	o.kvStore.Announce(encodedName)

	result := <-results
	return result, result == o.clientAddr, nil
}

func (o *Orchestrator) ParticipateInElection(partManager PartitionManager) {
	o.kvStore.ListenForAnnouncements(func(fullName string) {
		o.participateInElection(fullName, partManager)
	})
}

func (o *Orchestrator) participateInElection(fullName string, partManager PartitionManager) {
	o.log.Debug("(%s) Participate in election for %s", o.clientAddr, fullName)

	name, replica := o.decodeIndex(fullName)
	if !partManager.Participate(name, replica) {
		return
	}

	acquired := o.kvStore.Acquire(fullName)
	if !acquired {
		o.log.Debug("(%s) Lost election for %s", o.clientAddr, fullName)
		return
	}

	o.log.Debug("(%s) Won election for %s", o.clientAddr, fullName)
	o.kvStore.DeleteAnnouncement(fullName)

	prevReplica, prevReplicaNeeded := partManager.Add(name, replica)
	if prevReplicaNeeded {
		o.kvStore.Announce(o.encodeIndex(name, prevReplica))
	}

	if replica != 0 {
		return
	}

	for i := uint(1); i <= o.numberOfReplicas; i++ {
		o.kvStore.Announce(o.encodeIndex(name, i))
	}
}

func (o *Orchestrator) listenForReplicas(name string, callback func(name string, replica uint, addr string)) {
	o.kvStore.ListenForLeader(name, func(encodedName, addr string) {
		decodedName, replica := o.decodeIndex(encodedName)
		callback(decodedName, replica, addr)
	})
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
