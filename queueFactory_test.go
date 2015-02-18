package talaria_test

import (
	"fmt"
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
	Context("AddQueue and Fetch", func() {
		It("Should add a new queue", func() {
			factory := NewQueueFactory(10)
			err := factory.AddQueue("some-queue", 5)
			Expect(err).To(BeNil())
		})
		It("Should return a queue with the default buffer size", func() {
			factory := NewQueueFactory(10)
			err := factory.AddQueue("some-queue", AnyBufferSize)
			Expect(err).To(BeNil())
			queue := factory.Fetch("some-queue")
			Expect(queue.BufferSize()).To(BeEquivalentTo(10))
		})
		It("Should fail if a queue with the same name exists", func() {
			factory := NewQueueFactory(10)
			err := factory.AddQueue("some-queue", AnyBufferSize)
			Expect(err).To(BeNil())
			err = factory.AddQueue("some-queue", AnyBufferSize)
			Expect(err).ToNot(BeNil())
		})
		It("Should add in a thread safe way", func(done Done) {
			// NOTE: This test's real purpose is to let the --race flag fail
			// if it needs to
			defer close(done)
			factory := NewQueueFactory(10)
			results := make(chan Queue, 5)
			for i := 0; i < 5; i++ {
				go func(index int) {
					defer GinkgoRecover()
					name := fmt.Sprintf("some-queue-%d", index)
					err := factory.AddQueue(name, 5)
					Expect(err).To(BeNil())
					results <- factory.Fetch(name)
				}(i)
			}

			set := make(map[Queue]bool)
			for i := 0; i < 5; i++ {
				q := <-results
				set[q] = true
			}

			Expect(set).To(HaveLen(5))
		})
	})
	Context("RemoveQueue", func() {
		It("Should remove a queue", func() {
			factory := NewQueueFactory(10)
			err := factory.AddQueue("some-queue", 5)
			Expect(err).To(BeNil())
			queue1 := factory.Fetch("some-queue")
			factory.RemoveQueue("some-queue")
			err = factory.AddQueue("some-queue", 5)
			Expect(err).To(BeNil())
			queue2 := factory.Fetch("some-queue")
			Expect(queue1).ToNot(Equal(queue2))
		})
		It("Should remove in a thread safe way", func() {
			factory := NewQueueFactory(10)
			err := factory.AddQueue("some-queue", 5)
			queue1 := factory.Fetch("some-queue")
			Expect(err).To(BeNil())
			wg := &sync.WaitGroup{}

			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func() {
					factory.RemoveQueue("some-queue")
					wg.Done()
				}()
			}

			wg.Wait()
			err = factory.AddQueue("some-queue", 5)
			queue2 := factory.Fetch("some-queue")
			Expect(err).To(BeNil())
			Expect(queue1).ToNot(Equal(queue2))
		})
	})
	Context("ListQueues", func() {
		It("Should list all the queues", func() {
			factory := NewQueueFactory(10)
			factory.AddQueue("a", 5)
			factory.AddQueue("b", 5)
			factory.AddQueue("c", 5)
			names := make([]string, 0)
			for _, q := range factory.ListQueues() {
				names = append(names, q.Name)
			}
			Expect(names).To(ConsistOf([]string{"a", "b", "c"}))
		})
	})
})
