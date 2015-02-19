package restful_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/apoydence/talaria"
	"github.com/apoydence/talaria/neighbors"
	. "github.com/apoydence/talaria/restful"
	"github.com/gorilla/websocket"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RestServer - Single", func() {
	var httpServer *testHttpServer
	var mqh *mockQueueHolder

	BeforeEach(func() {
		mqh = newMockQueueHolder()
		nh := neighbors.NewNeighborCollection()
		httpServer = NewTestHttpServer()
		server := NewRestServer(mqh, nh, "")
		server.HttpServer = httpServer
		server.Start()
	})

	AfterEach(func() {
		httpServer.Close()
	})

	Context("AddQueue", func() {
		It("Should create a new queue", func() {
			qd := QueueData{
				QueueName: "some-queue",
				Buffer:    5,
			}
			resp, err := http.PostForm(httpServer.Endpoint()+"/queues", qd.ToUrlValues())
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(mqh.addQueueName).To(Equal("some-queue"))
			Expect(mqh.addBufferSize).To(BeEquivalentTo(5))
		})
	})
	Context("Fetch", func() {
		It("Should return information on the queue", func() {
			url := httpServer.Endpoint() + "/queues/queueOf5"
			resp, err := http.Get(url)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			dec := json.NewDecoder(resp.Body)
			var data QueueData
			err = dec.Decode(&data)
			Expect(err).To(BeNil())
			Expect(data.QueueName).To(Equal("queueOf5"))
			Expect(data.Buffer).To(BeEquivalentTo(5))
		})
		It("Should return information on all the queues", func() {
			url := httpServer.Endpoint() + "/queues"
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
			u := httpServer.Endpoint() + "/queues/someQueue"
			req, _ := http.NewRequest("DELETE", u, nil)
			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(mqh.removeQueueName).To(Equal("someQueue"))
		})
		It("Should return a BadRequest if the queue name is left off", func() {
			u := httpServer.Endpoint() + "/queues"
			req, _ := http.NewRequest("DELETE", u, nil)
			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
		})
	})
	Context("Read Data", func() {
		It("Should return a websocket that can read data from the queue", func(done Done) {
			defer close(done)
			url := "ws" + httpServer.Endpoint()[4:] + "/connect"
			dialer := &websocket.Dialer{}
			header := http.Header{}
			header.Add("read", "queueOf5")
			conn, _, err := dialer.Dial(url, header)
			Expect(err).To(BeNil())
			Expect(conn).ToNot(BeNil())
			defer conn.Close()

			messageType, data, err := conn.ReadMessage()
			Expect(messageType).To(Equal(websocket.BinaryMessage))
			Expect(err).To(BeNil())
			Expect(data).To(Equal([]byte{1, 2, 3, 4, 5}))

			err = conn.Close()
			Expect(err).To(BeNil())
		})
		It("Should close the connection when the queue is removed", func(done Done) {
			defer close(done)
			url := "ws" + httpServer.Endpoint()[4:] + "/connect"
			dialer := &websocket.Dialer{}
			header := http.Header{}
			header.Add("read", "queueWith5")
			conn, _, err := dialer.Dial(url, header)
			Expect(err).To(BeNil())
			Expect(conn).ToNot(BeNil())
			defer conn.Close()

			for i := 0; i < 5; i++ {
				messageType, data, err := conn.ReadMessage()
				Expect(messageType).To(Equal(websocket.BinaryMessage))
				Expect(err).To(BeNil())
				Expect(data).To(Equal([]byte{1, 2, 3, 4, 5}))
			}

			mqh.queueWith5.Close()
			mqh.queueWith5.Write([]byte{1, 2, 3, 4, 5})

			messageType, data, err := conn.ReadMessage()
			Expect(messageType).ToNot(Equal(websocket.BinaryMessage))
			Expect(err).ToNot(BeNil())
			Expect(data).To(BeNil())
		})
	})
	Context("Write Data", func() {
		It("Should return a websocket that can write data to the queue", func(done Done) {
			defer close(done)
			url := "ws" + httpServer.Endpoint()[4:] + "/connect"
			header := http.Header{}
			header.Add("write", "queueWriter")
			dialer := &websocket.Dialer{}
			conn, _, err := dialer.Dial(url, header)
			Expect(err).To(BeNil())
			Expect(conn).ToNot(BeNil())
			defer conn.Close()

			conn.WriteMessage(websocket.BinaryMessage, []byte{1, 2, 3, 4, 5})
			expectedData := mqh.queueWriter.Read()
			Expect(expectedData).To(Equal([]byte{1, 2, 3, 4, 5}))
		})
		It("Should close the connection when the queue is removed", func(done Done) {
			defer close(done)
			url := "ws" + httpServer.Endpoint()[4:] + "/connect"
			header := http.Header{}
			header.Add("write", "queueWith5")
			dialer := &websocket.Dialer{}
			conn, _, err := dialer.Dial(url, header)
			Expect(err).To(BeNil())
			Expect(conn).ToNot(BeNil())
			defer conn.Close()

			for i := 0; i < 5; i++ {
				err := conn.WriteMessage(websocket.BinaryMessage, []byte{1, 2, 3, 4, 5})
				Expect(err).To(BeNil())
			}

			mqh.queueWith5.Close()
			testIfCloses := func() error {
				return conn.WriteMessage(websocket.BinaryMessage, []byte{1, 2, 3, 4, 5})
			}
			Eventually(testIfCloses).ShouldNot(BeNil())
		})
	})
	Context("Read and Write Data", func() {
		It("Should read from one queue and write to another", func(done Done) {
			defer close(done)

			url := "ws" + httpServer.Endpoint()[4:] + "/connect"
			header := http.Header{}
			header.Add("read", "queueOf5")
			header.Add("write", "queueWriter")
			dialer := &websocket.Dialer{}
			conn, _, err := dialer.Dial(url, header)
			Expect(err).To(BeNil())
			Expect(conn).ToNot(BeNil())
			defer conn.Close()

			messageType, data, err := conn.ReadMessage()
			Expect(messageType).To(Equal(websocket.BinaryMessage))
			Expect(err).To(BeNil())
			Expect(data).To(Equal([]byte{1, 2, 3, 4, 5}))

			conn.WriteMessage(websocket.BinaryMessage, []byte{1, 2, 3, 4, 5})
			expectedData := mqh.queueWriter.Read()
			Expect(expectedData).To(Equal([]byte{1, 2, 3, 4, 5}))
		})
	})
})

var _ = Describe("RestServer - Distributed", func() {
	var httpServer *testHttpServer
	var httpClient *mockHttpClient
	var mqh *talaria.QueueFactory

	BeforeEach(func() {
		mqh = talaria.NewQueueFactory(10)
		httpServer = NewTestHttpServer()
	})

	AfterEach(func() {
		httpServer.Close()
	})

	setup := func(handler func(url string) (resp *http.Response, err error), endpoints ...string) {
		nh := neighbors.NewNeighborCollection(createFakeNeighbors(endpoints...)...)
		httpClient = NewMockHttpClient(len(endpoints), handler)
		server := NewRestServer(mqh, nh, "")
		server.HttpClient = httpClient
		server.HttpServer = httpServer
		server.Start()
	}

	Context("Fetch", func() {
		It("Should ask neighbors for queue", func(done Done) {
			defer close(done)
			setup(func(url string) (resp *http.Response, err error) {
				if strings.HasPrefix(url, "c") {
					data, _ := json.Marshal(NewQueueData("queueOf5", 5))
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       ioutil.NopCloser(bytes.NewReader(data)),
					}, nil
				}

				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       ioutil.NopCloser(strings.NewReader("")),
				}, nil
			}, "a", "b", "c")
			url := httpServer.Endpoint() + "/queues/queueOf5"

			resp, err := http.Get(url)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			dec := json.NewDecoder(resp.Body)
			var data QueueData
			err = dec.Decode(&data)
			Expect(err).To(BeNil())
			Expect(data.QueueName).To(Equal("queueOf5"))
			Expect(data.Buffer).To(BeEquivalentTo(5))

			results := make([]string, 0)
			for i := 0; i < 3; i++ {
				results = append(results, <-httpClient.getUrls)
			}
			Expect(results).To(ConsistOf([]string{"a/queues/queueOf5", "b/queues/queueOf5", "c/queues/queueOf5"}))
		})
	})
})

func createFakeNeighbors(endpoints ...string) []neighbors.Neighbor {
	result := make([]neighbors.Neighbor, 0)
	for _, e := range endpoints {
		result = append(result, neighbors.NewNeighbor(e))
	}
	return result
}

type mockHttpClient struct {
	getUrls chan string
	handler func(url string) (resp *http.Response, err error)
}

func NewMockHttpClient(chSize int, handler func(url string) (resp *http.Response, err error)) *mockHttpClient {
	return &mockHttpClient{
		getUrls: make(chan string, chSize),
		handler: handler,
	}
}

func (mc *mockHttpClient) Get(url string) (resp *http.Response, err error) {
	mc.getUrls <- url
	return mc.handler(url)
}

type mockQueueHolder struct {
	addQueueName    string
	removeQueueName string
	fetchQueueName  string
	addBufferSize   talaria.BufferSize
	queueOf5        talaria.Queue
	queueWith5      talaria.Queue
	queueWriter     talaria.Queue
	infiniteQueue   talaria.Queue
}

func newMockQueueHolder() *mockQueueHolder {
	queueWith5 := talaria.NewQueue(5)
	m := &mockQueueHolder{
		queueOf5:      talaria.NewQueue(5),
		queueWith5:    queueWith5,
		queueWriter:   talaria.NewQueue(5),
		infiniteQueue: talaria.NewQueue(5),
	}

	for i := 0; i < 5; i++ {
		if !queueWith5.Write([]byte{1, 2, 3, 4, 5}) {
			break
		}
	}
	return m
}

func (mqh *mockQueueHolder) AddQueue(queueName string, bufferSize talaria.BufferSize) error {
	mqh.addQueueName = queueName
	mqh.addBufferSize = bufferSize
	return nil
}

func (mqh *mockQueueHolder) Fetch(queueName string) talaria.Queue {
	mqh.fetchQueueName = queueName
	switch queueName {
	case "queueOf5":
		go func() {
			for {
				mqh.queueOf5.Write([]byte{1, 2, 3, 4, 5})
			}
		}()
		return mqh.queueOf5
	case "queueWith5":
		return mqh.queueWith5
	case "queueWriter":
		return mqh.queueWriter
	case "infiniteQueue":
		go func() {
			for {
				if !mqh.infiniteQueue.Write([]byte{1, 2, 3, 4, 5}) {
					break
				}
			}
		}()
		return mqh.infiniteQueue
	default:
		return nil
	}
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
