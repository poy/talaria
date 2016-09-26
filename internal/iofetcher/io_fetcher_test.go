package iofetcher_test

import (
	"github.com/apoydence/talaria/internal/buffers/ringbuffer"
	"github.com/apoydence/talaria/internal/iofetcher"
	"github.com/apoydence/talaria/internal/server"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("IoFetcher", func() {
	var (
		fetcher *iofetcher.IOFetcher
	)

	BeforeEach(func() {
		fetcher = iofetcher.New()
	})

	Describe("FetchReader()", func() {
		Context("Create() has been called", func() {
			BeforeEach(func() {
				Expect(fetcher.Create("some-buffer")).To(Succeed())
			})

			It("returns the same reader each time", func() {
				readerA, err := fetcher.FetchReader("some-buffer")
				Expect(err).ToNot(HaveOccurred())

				readerB, err := fetcher.FetchReader("some-buffer")
				Expect(err).ToNot(HaveOccurred())

				Expect(readerA).ToNot(BeNil())
				Expect(readerA).To(Equal(readerB))
			})

			Context("Create() has been called twice for the same buffer", func() {
				var (
					readerA server.Reader
				)

				BeforeEach(func() {
					readerA, _ = fetcher.FetchReader("some-buffer")
					readerA.(*ringbuffer.RingBuffer).Size = 99
					Expect(fetcher.Create("some-buffer")).To(Succeed())
				})

				It("still returns the same reader each time", func() {
					readerB, _ := fetcher.FetchReader("some-buffer")

					Expect(readerA).To(Equal(readerB))
				})
			})
		})

		Context("Create() has not been called", func() {
			It("returns an error", func() {
				_, err := fetcher.FetchReader("some-buffer")
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("FetchWriter()", func() {
		Context("Create() has been called", func() {
			BeforeEach(func() {
				Expect(fetcher.Create("some-buffer")).To(Succeed())
			})

			It("returns the same writer each time", func() {
				writerA, err := fetcher.FetchWriter("some-buffer")
				Expect(err).ToNot(HaveOccurred())

				writerB, err := fetcher.FetchWriter("some-buffer")
				Expect(err).ToNot(HaveOccurred())

				Expect(writerA).ToNot(BeNil())
				Expect(writerA).To(Equal(writerB))
			})
		})

		Context("Create() has not been called", func() {
			It("returns an error", func() {
				_, err := fetcher.FetchWriter("some-buffer")
				Expect(err).To(HaveOccurred())
			})
		})
	})

})
