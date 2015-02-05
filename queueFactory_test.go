package talaria_test

import (
	. "github.com/apoydence/talaria"
	"io"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("QueueFactory", func() {
	Context("Fetch", func() {
		It("Should add a new queue", func() {
			factory := NewQueueFactory()
			queue := factory.Fetch("some-queue")
			Expect(queue).ToNot(BeNil())
		})
		It("Should use an already present queue", func() {
			factory := NewQueueFactory()
			queue1 := factory.Fetch("some-queue")
			queue2 := factory.Fetch("some-queue")
			Expect(queue1).To(Equal(queue2))
		})
		It("Should add in a thread safe way", func() {
			factory := NewQueueFactory()
			results := make(chan io.ReadWriteCloser, 5)
			for i := 0; i < 5; i++ {
				go func() {
					results <- factory.Fetch("some-queue")
				}()
			}

			set := make(map[io.ReadWriteCloser]bool)
			for i := 0; i < 5; i++ {
				q := <-results
				set[q] = true
			}

			Expect(set).To(HaveLen(1))
		})
	})
	Context("Remove", func() {
		It("Should remove a queue", func() {
			factory := NewQueueFactory()
			queue1 := factory.Fetch("some-queue")
			factory.Remove("some-queue")
			queue2 := factory.Fetch("some-queue")
			Expect(queue1).ToNot(Equal(queue2))
		})
		It("Should remove in a thread safe way", func() {
			factory := NewQueueFactory()
			queue1 := factory.Fetch("some-queue")
			wg := &sync.WaitGroup{}

			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func() {
					factory.Remove("some-queue")
					wg.Done()
				}()
			}

			wg.Wait()
			queue2 := factory.Fetch("some-queue")
			Expect(queue1).ToNot(Equal(queue2))
		})
	})
})
