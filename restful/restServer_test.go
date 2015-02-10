package restful_test

import (
	"bytes"
	"encoding/json"
	"github.com/apoydence/talaria"
	. "github.com/apoydence/talaria/restful"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var mqh = func() *mockQueueHolder {
	mqh := &mockQueueHolder{}
	StartNewRestServer(mqh, ":8080")
	return mqh
}()

var _ = Describe("RestServer", func() {
	Context("AddQueue", func() {
		It("Should create a new queue", func() {
			j, _ := json.Marshal(QueueData{
				QueueName: "some-queue",
				Buffer:    5,
			})
			resp, err := http.Post("http://localhost:8080/queues", "application/json", bytes.NewReader(j))
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(mqh.addQueueName).To(Equal("some-queue"))
			Expect(mqh.addBufferSize).To(BeEquivalentTo(5))
		})
	})
	Context("Fetch", func() {
		It("Should return information on the queue", func() {
			url := "http://localhost:8080/queues/someQueue"
			resp, err := http.Get(url)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			dec := json.NewDecoder(resp.Body)
			var data QueueData
			err = dec.Decode(&data)
			Expect(err).To(BeNil())
			Expect(data.QueueName).To(Equal("someQueue"))
			Expect(data.Buffer).To(BeEquivalentTo(6))
		})
	})
	Context("Remove", func() {
		It("Should call Remove on the QueueHolder", func() {
			url := "http://localhost:8080/queues/someQueue"
			req, _ := http.NewRequest("DELETE", url, nil)
			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(mqh.removeQueueName).To(Equal("someQueue"))
		})
		It("Should return a BadRequest if the queue name is left off", func() {
			url := "http://localhost:8080/queues"
			req, _ := http.NewRequest("DELETE", url, nil)
			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
		})
	})
})

type mockQueueHolder struct {
	addQueueName    string
	removeQueueName string
	fetchQueueName  string
	addBufferSize   talaria.BufferSize
}

func (mqh *mockQueueHolder) AddQueue(queueName string, bufferSize talaria.BufferSize) error {
	mqh.addQueueName = queueName
	mqh.addBufferSize = bufferSize
	return nil
}

func (mqh *mockQueueHolder) Fetch(queueName string) talaria.Queue {
	mqh.fetchQueueName = queueName
	return talaria.NewQueue(6)
}

func (mqh *mockQueueHolder) RemoveQueue(queueName string) {
	mqh.removeQueueName = queueName
}