package orchestrator

type PartitionManager interface {
	Add(name string)
}

type KvStore interface {
	Announce(name string)
	Acquire(name string) bool
	ListenForAnnouncements(callback func(name string))
	ListenForLeader(name string, callback func(name, uri string))
	FetchLeader(name string) (string, bool)
}

type Orchestrator struct {
	partManager PartitionManager
	kvStore     KvStore
	clientAddr  string
}

func New(clientAddr string, partManager PartitionManager, kvStore KvStore) *Orchestrator {
	orch := &Orchestrator{
		partManager: partManager,
		kvStore:     kvStore,
		clientAddr:  clientAddr,
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

	o.kvStore.Announce(name)

	result := <-results
	return result, result == o.clientAddr
}

func (o *Orchestrator) participateInElection(name string) {
	acquired := o.kvStore.Acquire(name)
	if !acquired {
		return
	}

	o.partManager.Add(name)
}
