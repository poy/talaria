package talaria_test

import (
	. "github.com/apoydence/talaria"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Queue", func() {
	Context("BufferSize", func() {
		It("Should return the buffer length of the queue", func() {
			q := NewQueue(44)
			Expect(q.BufferSize()).To(BeEquivalentTo(44))
		})
	})
	Context("Read and Write", func() {
		It("Should write an array to the channel", func(done Done) {
			defer close(done)
			queue := NewQueue(5)
			data := []byte{1, 2, 3, 4, 5}
			queue.Write(data)
			result := queue.Read()
			Expect(data).To(Equal(result))
		})
		It("Should return an error when you write to a closed channel", func(done Done) {
			defer close(done)
			queue := NewQueue(5)
			queue.Close()
			data := []byte{1, 2, 3, 4, 5}
			queue.Write(data)
		})
		It("Queue can be written to by multiple go routines", func(done Done) {
			defer close(done)
			queue := NewQueue(5)
			wg := &sync.WaitGroup{}
			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					for j := 0; j < 5; j++ {
						data := []byte{1, 2, 3, 4, 5}
						queue.Write(data)
					}
					wg.Done()
				}()
			}

			go func() {
				wg.Wait()
				queue.Close()
			}()

			count := 0
			for d := queue.Read(); d != nil; d = queue.Read() {
				count++
			}
			Expect(count).To(Equal(25))
		})
	})
	Context("ReadAsync", func() {
		It("Should return a nil if there is nothing to read", func(done Done) {
			defer close(done)
			queue := NewQueue(5)
			data := queue.ReadAsync()
			Expect(data).To(BeNil())
		})
	})
	Context("Close", func() {
		It("Should close the queue", func(done Done) {
			defer close(done)
			queue := NewQueue(5)
			queue.Close()
			data := queue.Read()
			Expect(data).To(BeNil())
		})
	})
})
