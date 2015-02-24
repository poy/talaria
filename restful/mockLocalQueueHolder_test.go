package restful_test

import "github.com/apoydence/talaria"

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

func (m *mockLocalQueueHolder) ListQueues() []talaria.QueueListing {
	results := make([]talaria.QueueListing, 0)
	for k, v := range m.queues {
		results = append(results, talaria.QueueListing{
			Name: k,
			Q:    v,
		})
	}
	return results
}
