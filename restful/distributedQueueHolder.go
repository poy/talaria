package restful

import (
	"encoding/json"
	"fmt"
	"github.com/apoydence/talaria"
	"github.com/apoydence/talaria/neighbors"
	"net/http"
	"sync"
)

type LocalQueueHolder interface {
	QueueHolder
	Fetch(queueName string) talaria.Queue
}

type NeighborHolder interface {
	GetNeighbors(blacklist ...string) []neighbors.Neighbor
}

type HttpClient interface {
	Get(url string, header http.Header) (resp *http.Response, err error)
	Delete(url string, header http.Header) (resp *http.Response, err error)
}

type distQueueHolder struct {
	HttpClient       HttpClient
	endpoint         string
	localQueueHolder LocalQueueHolder
	neighborHolder   NeighborHolder
}

func NewDistributedQueueHolder(endpoint string, localQueueHolder LocalQueueHolder, neighborHolder NeighborHolder) *distQueueHolder {
	return &distQueueHolder{
		endpoint:         endpoint,
		localQueueHolder: localQueueHolder,
		neighborHolder:   neighborHolder,
		HttpClient:       NewDefaultHttpClient(),
	}
}

func (dqh *distQueueHolder) AddQueue(queueName string, size talaria.BufferSize) error {
	return nil
}

func (dqh *distQueueHolder) Fetch(queueName string, blacklist ...string) (talaria.Queue, string, int) {
	localQueue := dqh.localQueueHolder.Fetch(queueName)
	if localQueue != nil {
		return localQueue, "", 200
	}

	remoteEndpoint, statusCode := func() (string, int) {
		headers, ns := dqh.buildBlacklist(blacklist)
		for _, neighbor := range ns {
			url := fmt.Sprintf("%s/queues/%s", neighbor, queueName)
			resp, err := dqh.HttpClient.Get(url, headers)
			if err == nil && resp.StatusCode == http.StatusOK {
				return neighbor, http.StatusOK
			}
		}
		return "", http.StatusNotFound
	}()

	return nil, remoteEndpoint, statusCode
}

func (dqh *distQueueHolder) RemoveQueue(queueName string) {
	queue, remoteEndpoint, statusCode := dqh.Fetch(queueName)
	if queue != nil {
		dqh.localQueueHolder.RemoveQueue(queueName)
	} else if statusCode == http.StatusOK {
		dqh.HttpClient.Delete(fmt.Sprintf("%s/queues/%s", remoteEndpoint, queueName), nil)
	}
}

func (dqh *distQueueHolder) ListQueues(blacklist ...string) chan QueueData {
	results := make(chan QueueData)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for q := range dqh.localQueueHolder.ListQueues() {
			results <- q
		}
		wg.Done()
	}()

	headers, ns := dqh.buildBlacklist(blacklist)

	for _, neighbor := range ns {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			resp, err := dqh.HttpClient.Get(endpoint, headers)
			if err == nil && resp != nil {
				dec := json.NewDecoder(resp.Body)
				var data QueueData
				err = dec.Decode(&data)
				if err == nil {
					results <- data
				}
			}
		}(neighbor)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

func (dqh *distQueueHolder) buildBlacklist(blacklist []string) (http.Header, []string) {
	headers := make(map[string][]string)
	ns := make([]string, 0)
	for _, neighbor := range dqh.neighborHolder.GetNeighbors(blacklist...) {
		ns = append(ns, neighbor.Endpoint)
		blacklist = append(blacklist, neighbor.Endpoint)
	}
	blacklist = append(blacklist, dqh.endpoint)
	headers["blacklist"] = blacklist
	return headers, ns
}
