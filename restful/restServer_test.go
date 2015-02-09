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
	Context("Fetch", func() {
		It("Should create a new queue", func() {
			mqh := &mockQueueHolder{}
			_, errChan := StartNewRestServer(mqh, ":8080")
			resp, err := http.PostForm("http://localhost:8080/fetch", url.Values{
				"name":       {"some-queue"},
				"bufferSize": {"5"},
			})
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(Equal(200))
			Expect(errChan).ShouldNot(Receive())
			Expect(mqh.fetchQueueName).To(Equal("some-queue"))
			Expect(mqh.fetchBufferSize).To(BeEquivalentTo(5))
		})
	})
})

type mockQueueHolder struct {
	fetchQueueName  string
	fetchBufferSize talaria.BufferSize
}

func (mqh *mockQueueHolder) Fetch(queueName string, bufferSize talaria.BufferSize) (talaria.Queue, error) {
	mqh.fetchQueueName = queueName
	mqh.fetchBufferSize = bufferSize
	return nil, nil
}
