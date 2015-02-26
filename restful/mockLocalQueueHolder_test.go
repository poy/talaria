package restful_test

import (
	"github.com/apoydence/talaria"
	"github.com/apoydence/talaria/restful"
)

type mockLocalQueueHolder struct {
	queues          map[string]talaria.Queue
	removeQueueName string
}

func NewMockLocalQueueHolder(queues map[string]talaria.Queue) *mockLocalQueueHolder {
	return &mockLocalQueueHolder{
		queues: queues,
	}
}

func (m *mockLocalQueueHolder) AddQueue(queueName string, size talaria.BufferSize) error {
	return nil
}

func (m *mockLocalQueueHolder) Fetch(queueName string) talaria.Queue {
	if q, ok := m.queues[queueName]; ok {
		return q
	}
	return nil
}

func (m *mockLocalQueueHolder) RemoveQueue(queueName string) {
	m.removeQueueName = queueName
}

func (m *mockLocalQueueHolder) ListQueues(blacklist ...string) chan restful.QueueData {
	results := make(chan restful.QueueData)
	go func() {
		defer close(results)
		for k, v := range m.queues {
			results <- restful.NewQueueData(k, v.BufferSize(), "localhost")
		}
	}()
	return results
}
