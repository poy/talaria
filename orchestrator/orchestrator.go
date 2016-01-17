package orchestrator

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apoydence/talaria/logging"
)

type LeaderValidator interface {
	Validate(name string, index uint64, callback func(string, bool))
}

type IndexProvider interface {
	ProvideLastIndex(name string) (uint64, bool)
}

type PartitionManager interface {
	Participate(name string, index uint) bool
	Add(name string, replica uint) (uint, bool)
}

type KvStore interface {
	Announce(name string)
	Acquire(name string) bool
	Release(name string)
	ListenForAnnouncements(callback func(name string))
	ListenForLeader(name string, callback func(name, uri string))
	FetchLeader(name string) (string, bool)
}

type Orchestrator struct {
	log             logging.Logger
	kvStore         KvStore
	partManager     PartitionManager
	leaderValidator LeaderValidator
	indexProvider   IndexProvider

	clientAddr       string
	numberOfReplicas uint
}

func New(clientAddr string, numberOfReplicas uint, kvStore KvStore, partitionManager PartitionManager, leaderValidator LeaderValidator, indexProvider IndexProvider) *Orchestrator {
	orch := &Orchestrator{
		log:              logging.Log("Orchestrator"),
		kvStore:          kvStore,
		clientAddr:       clientAddr,
		numberOfReplicas: numberOfReplicas,
		partManager:      partitionManager,
		leaderValidator:  leaderValidator,
		indexProvider:    indexProvider,
	}

	return orch
}

func (o *Orchestrator) FetchLeader(name string, create bool) (string, bool, error) {
	encodedName := o.encodeIndex(name, 0)

	uri, ok := o.kvStore.FetchLeader(encodedName)
	if ok {
		return uri, o.clientAddr == uri, nil
	}

	if !create {
		return "", false, fmt.Errorf("File (%s) does not exist", name)
	}

	o.log.Debug("Create %s", name)

	results := make(chan string, 1)

	o.kvStore.ListenForLeader(encodedName, func(name, uri string) {
		results <- uri
	})

	o.kvStore.Announce(encodedName)

	result := <-results
	if result != o.clientAddr {
		return result, false, nil
	}

	for o.partManager.Participate(name, 0) {
		time.Sleep(time.Second)
		//return "", false, fmt.Errorf("leader not yet validated")
	}

	return result, true, nil
}

func (o *Orchestrator) ParticipateInElections() {
	o.kvStore.ListenForAnnouncements(func(fullName string) {
		o.participateInElection(fullName, o.partManager)
	})
}

func (o *Orchestrator) participateInElection(fullName string, partManager PartitionManager) {
	name, replica := o.decodeIndex(fullName)
	if !partManager.Participate(name, replica) {
		return
	}

	o.log.Debug("(%s) Participate in election for %s", o.clientAddr, fullName)

	acquired := o.kvStore.Acquire(fullName)
	if !acquired {
		o.log.Debug("(%s) Lost election for %s", o.clientAddr, fullName)
		return
	}

	o.log.Debug("(%s) Won election for %s", o.clientAddr, fullName)

	if replica != 0 {
		return
	}

	index, _ := o.indexProvider.ProvideLastIndex(name)

	o.leaderValidator.Validate(name, index, func(name string, validated bool) {
		if !validated {
			o.kvStore.Release(o.encodeIndex(name, 0))
			return
		}

		prevReplica, prevReplicaNeeded := partManager.Add(name, replica)

		if prevReplicaNeeded {
			prevFullName := o.encodeIndex(name, prevReplica)
			o.kvStore.Announce(prevFullName)
			o.kvStore.Release(prevFullName)
		}

		if replica != 0 {
			return
		}

		for i := uint(1); i <= o.numberOfReplicas; i++ {
			o.kvStore.Announce(o.encodeIndex(name, i))
		}
	})
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
