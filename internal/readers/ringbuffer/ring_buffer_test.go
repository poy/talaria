package ringbuffer_test

import (
	"io"

	"github.com/apoydence/talaria/internal/readers/ringbuffer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RingBuffer", func() {
	var (
		d     *ringbuffer.RingBuffer
		value []byte
	)

	Context("write data", func() {
		BeforeEach(func() {
			d = ringbuffer.New(5)

			value = []byte("some-value")
			d.WriteTo(value)
		})

		Context("multiple writes", func() {
			var (
				secondValue []byte
			)

			BeforeEach(func() {
				secondValue = []byte("some-other-value")
				d.WriteTo(secondValue)
			})

			Describe("Write()", func() {
				It("returns the written index", func() {
					idx, err := d.WriteTo(secondValue)
					Expect(err).ToNot(HaveOccurred())
					Expect(idx).To(BeEquivalentTo(2))
				})
			})

			Describe("Read()", func() {
				It("returns expected value", func() {
					data, idx, err := d.ReadAt(0)
					Expect(err).ToNot(HaveOccurred())
					Expect(data).To(Equal(value))
					Expect(idx).To(BeEquivalentTo(0))
				})

				Context("reads exceed writes", func() {
					It("returns io.EOF", func() {
						_, _, err := d.ReadAt(2)
						Expect(err).To(MatchError(io.EOF))
					})
				})
			})

			Context("buffer size exceeded", func() {
				BeforeEach(func() {
					for i := 0; i < 4; i++ {
						d.WriteTo(secondValue)
					}
				})

				It("wraps", func() {
					data, idx, err := d.ReadAt(0)
					Expect(err).ToNot(HaveOccurred())
					Expect(data).To(Equal(secondValue))
					Expect(idx).To(BeEquivalentTo(5))
				})
			})
		})
	})
})
