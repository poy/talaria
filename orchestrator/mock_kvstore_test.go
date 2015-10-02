package orchestrator_test

type mockKvStore struct {
	announceCh             chan string
	acquireRx              chan string
	acquireTx              chan bool
	fetchLeaderRx          chan string
	fetchLeaderTx          chan string
	announceLeaderTx       chan string
	fetchLeaderOk          chan bool
	listenNameCh           chan string
	leaderCallbackCh       chan func(name, uri string)
	announcementCallbackCh chan func(name string)
}

func newMockKvStore() *mockKvStore {
	return &mockKvStore{
		announceCh:             make(chan string, 100),
		acquireRx:              make(chan string, 100),
		acquireTx:              make(chan bool, 100),
		fetchLeaderRx:          make(chan string, 100),
		fetchLeaderTx:          make(chan string, 100),
		announceLeaderTx:       make(chan string, 100),
		fetchLeaderOk:          make(chan bool, 100),
		listenNameCh:           make(chan string, 100),
		leaderCallbackCh:       make(chan func(name, uri string), 100),
		announcementCallbackCh: make(chan func(name string), 100),
	}
}

func (m *mockKvStore) Announce(name string) {
	m.announceCh <- name
}

func (m *mockKvStore) FetchLeader(name string) (string, bool) {
	m.fetchLeaderRx <- name
	return <-m.fetchLeaderTx, <-m.fetchLeaderOk
}

func (m *mockKvStore) Acquire(name string) bool {
	m.acquireRx <- name
	return <-m.acquireTx
}

func (m *mockKvStore) ListenForLeader(name string, callback func(name, uri string)) {
	go func() {
		m.listenNameCh <- name
		m.leaderCallbackCh <- callback
		callback(name, <-m.fetchLeaderTx)
	}()
}

func (m *mockKvStore) ListenForAnnouncements(callback func(name string)) {
	go func() {
		for {
			m.announcementCallbackCh <- callback
			callback(<-m.announceLeaderTx)
		}
	}()
}
