package talaria_test

import (
	. "github.com/apoydence/talaria"

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
			err := queue.Write(data)
			Expect(err).To(BeNil())
			result := queue.Read()
			Expect(data).To(Equal(result))
		})
		It("Should read a nil if there is nothing to read", func(done Done) {
			defer close(done)
			queue := NewQueue(5)
			data := queue.Read()
			Expect(data).To(BeNil())
		})
	})
})
