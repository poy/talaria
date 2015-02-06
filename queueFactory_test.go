package talaria_test

import (
	. "github.com/apoydence/talaria"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("QueueFactory", func() {
	Context("NewQueueFactory", func() {
		It("Should panic with a negative buffer size", func() {
			Expect(func() { NewQueueFactory(AnyBufferSize) }).Should(Panic())
		})
	})
	Context("Fetch", func() {
		It("Should add a new queue", func() {
			factory := NewQueueFactory(10)
			queue, err := factory.Fetch("some-queue", 5)
			Expect(err).To(BeNil())
			Expect(queue).ToNot(BeNil())
		})
		It("Should use an already present queue", func() {
			factory := NewQueueFactory(10)
			queue1, err := factory.Fetch("some-queue", 5)
			Expect(err).To(BeNil())
			queue2, err := factory.Fetch("some-queue", 5)
			Expect(err).To(BeNil())
			Expect(queue1).To(Equal(queue2))
		})
		It("Should return an error if different buffer sizes are required", func() {
			factory := NewQueueFactory(10)
			_, err := factory.Fetch("some-queue", 5)
			Expect(err).To(BeNil())
			_, err = factory.Fetch("some-queue", 10)
			Expect(err).ToNot(BeNil())
		})
		It("Should return the existing queue with any buffer size", func() {
			factory := NewQueueFactory(10)
			queue1, err := factory.Fetch("some-queue", 5)
			Expect(err).To(BeNil())
			queue2, err := factory.Fetch("some-queue", AnyBufferSize)
			Expect(err).To(BeNil())
			Expect(queue1).To(Equal(queue2))
			Expect(queue1.BufferSize()).To(BeEquivalentTo(5))
		})
		It("Should return a queue with the default buffer size", func() {
			factory := NewQueueFactory(10)
			queue, err := factory.Fetch("some-queue", AnyBufferSize)
			Expect(err).To(BeNil())
			Expect(queue.BufferSize()).To(BeEquivalentTo(10))
		})
		It("Should add in a thread safe way", func() {
			factory := NewQueueFactory(10)
			results := make(chan Queue, 5)
			for i := 0; i < 5; i++ {
				go func() {
					q, err := factory.Fetch("some-queue", 5)
					Expect(err).To(BeNil())
					results <- q
				}()
			}

			set := make(map[Queue]bool)
			for i := 0; i < 5; i++ {
				q := <-results
				set[q] = true
			}

			Expect(set).To(HaveLen(1))
		})
	})
	Context("Remove", func() {
		It("Should remove a queue", func() {
			factory := NewQueueFactory(10)
			queue1, err := factory.Fetch("some-queue", 5)
			Expect(err).To(BeNil())
			factory.Remove("some-queue")
			queue2, err := factory.Fetch("some-queue", 5)
			Expect(err).To(BeNil())
			Expect(queue1).ToNot(Equal(queue2))
		})
		It("Should remove in a thread safe way", func() {
			factory := NewQueueFactory(10)
			queue1, err := factory.Fetch("some-queue", 5)
			Expect(err).To(BeNil())
			wg := &sync.WaitGroup{}

			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func() {
					factory.Remove("some-queue")
					wg.Done()
				}()
			}

			wg.Wait()
			queue2, err := factory.Fetch("some-queue", 5)
			Expect(err).To(BeNil())
			Expect(queue1).ToNot(Equal(queue2))
		})
	})
	Context("ListQueues", func() {
		It("Should list all the queues", func() {
			factory := NewQueueFactory(10)
			factory.Fetch("a", 5)
			factory.Fetch("b", 5)
			factory.Fetch("c", 5)
			names := make([]string, 0)
			for _, q := range factory.ListQueues() {
				names = append(names, q.Name)
			}
			Expect(names).To(ConsistOf([]string{"a", "b", "c"}))
		})
	})
})
