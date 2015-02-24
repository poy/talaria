package restful_test

import (
	"github.com/apoydence/talaria"
	. "github.com/apoydence/talaria/restful"
	"net/http"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = FDescribe("DistributedQueueHolder", func() {
	Context("Fetch", func() {
		It("Should return a local queue", func() {
			queues := make(map[string]talaria.Queue)
			queues["some-queue"] = talaria.NewQueue(5)
			localQueueHolder := NewMockLocalQueueHolder(queues)
			var holder RemoteQueueHolder = NewDistributedQueueHolder("", localQueueHolder, nil)
			localQueue, remoteEndpoint, statusCode := holder.Fetch("some-queue")

			Expect(localQueue).ToNot(BeNil())
			Expect(remoteEndpoint).To(Equal(""))
			Expect(statusCode).To(Equal(http.StatusOK))
		})
		It("Should return a remote queue", func() {
			queues := make(map[string]talaria.Queue)
			chHeaders := make(chan http.Header, 3)
			mockClient := NewTestHttpClient(func(url, method string, header http.Header) (*http.Response, error) {
				chHeaders <- header
				if method == "GET" && strings.HasPrefix(url, "endpoint-c") {
					return &http.Response{
						StatusCode: 200,
					}, nil
				}
				return &http.Response{}, nil
			})
			localQueueHolder := NewMockLocalQueueHolder(queues)
			neighborHolder := NewMockNeighborHolder("endpoint-a", "endpoint-b", "endpoint-c")
			holder := NewDistributedQueueHolder("endpoint-x", localQueueHolder, neighborHolder)
			holder.HttpClient = mockClient
			queue, remoteEndpoint, statusCode := holder.Fetch("some-queue")

			Expect(queue).To(BeNil())
			Expect(statusCode).To(Equal(http.StatusOK))
			Expect(remoteEndpoint).To(Equal("endpoint-c"))
			for i := 0; i < 3; i++ {
				Eventually(chHeaders).Should(Receive(HaveKeyWithValue("blacklist", []string{"endpoint-a", "endpoint-b", "endpoint-c", "endpoint-x"})))
			}
		})
		It("Should not request from a endpoint on the blacklist", func(done Done) {
			defer close(done)
			queues := make(map[string]talaria.Queue)
			mockClient := NewTestHttpClient(func(url, method string, header http.Header) (*http.Response, error) {
				return &http.Response{}, nil
			})

			localQueueHolder := NewMockLocalQueueHolder(queues)
			neighborHolder := NewMockNeighborHolder("endpoint-a", "endpoint-b", "endpoint-c")
			holder := NewDistributedQueueHolder("endpoint-x", localQueueHolder, neighborHolder)
			holder.HttpClient = mockClient
			holder.Fetch("some-queue", "endpoint-b")
			Expect(neighborHolder.lastBlacklist).To(Equal([]string{"endpoint-b"}))
		})
		It("Should return a 404 if the queue doesn't exist", func() {
			queues := make(map[string]talaria.Queue)
			mockClient := NewTestHttpClient(func(url, method string, header http.Header) (*http.Response, error) {
				return &http.Response{}, nil
			})
			localQueueHolder := NewMockLocalQueueHolder(queues)
			neighborHolder := NewMockNeighborHolder("endpoint-a", "endpoint-b", "endpoint-c")
			holder := NewDistributedQueueHolder("endpoint-x", localQueueHolder, neighborHolder)
			holder.HttpClient = mockClient
			queue, remoteEndpoint, statusCode := holder.Fetch("some-queue")

			Expect(queue).To(BeNil())
			Expect(statusCode).To(Equal(http.StatusNotFound))
			Expect(remoteEndpoint).To(Equal(""))
		})
	})
	Context("RemoveQueue", func() {
		It("Should remove a local queue", func() {
			queues := make(map[string]talaria.Queue)
			queues["some-queue"] = talaria.NewQueue(5)
			localQueueHolder := NewMockLocalQueueHolder(queues)
			var holder RemoteQueueHolder = NewDistributedQueueHolder("endpoint-x", localQueueHolder, nil)
			holder.RemoveQueue("some-queue")

			Expect(localQueueHolder.removeQueueName).To(Equal("some-queue"))
		})
		It("Should remove a remote queue", func() {
			queues := make(map[string]talaria.Queue)
			endpoints := make(chan string, 3)
			chHeaders := make(chan http.Header, 3)
			mockClient := NewTestHttpClient(func(url, method string, header http.Header) (*http.Response, error) {
				chHeaders <- header
				if strings.HasPrefix(url, "endpoint-b") {
					if method == "DELETE" {
						endpoints <- url
					}
					return &http.Response{
						StatusCode: 200,
					}, nil
				}
				return &http.Response{}, nil
			})
			localQueueHolder := NewMockLocalQueueHolder(queues)
			neighborHolder := NewMockNeighborHolder("endpoint-a", "endpoint-b", "endpoint-c")
			holder := NewDistributedQueueHolder("endpoint-x", localQueueHolder, neighborHolder)
			holder.HttpClient = mockClient
			holder.RemoveQueue("some-queue")

			Expect(endpoints).To(Receive(Equal("endpoint-b/queues/some-queue")))
		})
	})
	Context("ListQueues", func() {
		It("Should list the local queues", func() {
			queues := make(map[string]talaria.Queue)
			queues["queue-a"] = talaria.NewQueue(5)
			queues["queue-b"] = talaria.NewQueue(5)
			queues["queue-c"] = talaria.NewQueue(5)
			neighborHolder := NewMockNeighborHolder()
			localQueueHolder := NewMockLocalQueueHolder(queues)
			holder := NewDistributedQueueHolder("endpoint-x", localQueueHolder, neighborHolder)
			for _, q := range holder.ListQueues() {
				v, ok := queues[q.Name]
				Expect(ok).To(BeTrue())
				Expect(v).ToNot(BeNil())
				Expect(q.Q).To(Equal(v))
				queues[q.Name] = nil
			}

			for _, v := range queues {
				Expect(v).To(BeNil())
			}
		})
	})
})
