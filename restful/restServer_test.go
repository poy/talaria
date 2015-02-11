package restful_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/apoydence/talaria"
	. "github.com/apoydence/talaria/restful"
	"github.com/gorilla/websocket"
	"io"
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
		It("Should return information on all the queues", func() {
			url := "http://localhost:8080/queues"
			resp, err := http.Get(url)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			ds := readData(resp.Body)
			for i, d := range ds {
				Expect(d.QueueName).To(Equal(fmt.Sprintf("some-queue-%d", i)))
				Expect(d.Buffer).To(BeEquivalentTo(i))
			}
		})
	})
	Context("Remove", func() {
		It("Should call Remove on the QueueHolder", func() {
			u := "http://localhost:8080/queues/someQueue"
			req, _ := http.NewRequest("DELETE", u, nil)
			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(mqh.removeQueueName).To(Equal("someQueue"))
		})
		It("Should return a BadRequest if the queue name is left off", func() {
			u := "http://localhost:8080/queues"
			req, _ := http.NewRequest("DELETE", u, nil)
			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
		})
	})
	Context("ReadData", func() {
		It("Should return a websocket that can read data from the queue", func(done Done) {
			defer close(done)
			u := "ws://localhost:8080/queues/someQueue/readData"
			dialer := &websocket.Dialer{}
			conn, _, err := dialer.Dial(u, nil)
			Expect(err).To(BeNil())
			Expect(conn).ToNot(BeNil())

			messageType, data, err := conn.ReadMessage()
			Expect(messageType).To(Equal(websocket.BinaryMessage))
			Expect(err).To(BeNil())
			Expect(data).To(Equal([]byte{1, 2, 3, 4, 5}))

			err = conn.Close()
			Expect(err).To(BeNil())
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
	queue := talaria.NewQueue(6)
	queue.Write([]byte{1, 2, 3, 4, 5})
	return queue
}

func (mqh *mockQueueHolder) RemoveQueue(queueName string) {
	mqh.removeQueueName = queueName
}

func (mqh *mockQueueHolder) ListQueues() []talaria.QueueListing {
	qs := make([]talaria.QueueListing, 0)
	for i := 0; i < 3; i++ {
		qs = append(qs, talaria.QueueListing{
			Name: fmt.Sprintf("some-queue-%d", i),
			Q:    talaria.NewQueue(talaria.BufferSize(i)),
		})
	}
	return qs
}

func readData(reader io.Reader) []QueueData {
	ds := make([]QueueData, 0)
	var data QueueData
	dec := json.NewDecoder(reader)
	var err error
	for {
		if err = dec.Decode(&data); err != nil {
			break
		}
		ds = append(ds, data)
	}

	return ds
}
