package restful_test

import (
	"github.com/apoydence/talaria"
	. "github.com/apoydence/talaria/restful"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RestServer", func() {
	Context("AddQueue", func() {
		It("Should create a new queue", func() {
			mqh := &mockQueueHolder{}
			_, errChan := StartNewRestServer(mqh, ":8080")
			resp, err := http.PostForm("http://localhost:8080/addQueue", url.Values{
				"name":       {"some-queue"},
				"bufferSize": {"5"},
			})
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(200))
			Expect(errChan).ShouldNot(Receive())
			Expect(mqh.addQueueName).To(Equal("some-queue"))
			Expect(mqh.addBufferSize).To(BeEquivalentTo(5))
		})
	})
})

type mockQueueHolder struct {
	addQueueName   string
	fetchQueueName string
	addBufferSize  talaria.BufferSize
}

func (mqh *mockQueueHolder) AddQueue(queueName string, bufferSize talaria.BufferSize) error {
	mqh.addQueueName = queueName
	mqh.addBufferSize = bufferSize
	return nil
}

func (mqh *mockQueueHolder) Fetch(queueName string) talaria.Queue {
	mqh.fetchQueueName = queueName
	return nil
}
