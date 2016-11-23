package iofetcher_test

import (
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/buffers/ringbuffer"
	"github.com/apoydence/talaria/node/internal/iofetcher"
	"github.com/apoydence/talaria/node/internal/server"
)

type TT struct {
	*testing.T
	fetcher *iofetcher.IOFetcher
	readerA server.Reader
}

func TestIOFetcherFetchReader(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		return TT{
			T:       t,
			fetcher: iofetcher.New(),
		}
	})

	o.Group("when Create() has been called", func() {
		o.BeforeEach(func(t TT) TT {
			err := t.fetcher.Create("some-buffer")
			Expect(t, err == nil).To(Equal(true))
			return t
		})

		o.Spec("it returns the same reader each time", func(t TT) {
			readerA, err := t.fetcher.FetchReader("some-buffer")
			Expect(t, err == nil).To(Equal(true))

			readerB, err := t.fetcher.FetchReader("some-buffer")
			Expect(t, err == nil).To(Equal(true))

			Expect(t, readerA != nil).To(Equal(true))
			Expect(t, readerA).To(Equal(readerB))
		})

		o.Group("when Create() has been called twice for the same buffer", func() {
			o.BeforeEach(func(t TT) TT {
				readerA, _ := t.fetcher.FetchReader("some-buffer")
				readerA.(*ringbuffer.RingBuffer).Size = 99
				err := t.fetcher.Create("some-buffer")
				Expect(t, err == nil).To(Equal(true))
				t.readerA = readerA
				return t
			})

			o.Spec("it still returns the same reader each time", func(t TT) {
				readerB, _ := t.fetcher.FetchReader("some-buffer")

				Expect(t, t.readerA).To(Equal(readerB))
			})
		})
	})

	o.Group("when Create() has not been called", func() {
		o.Spec("it returns an error", func(t TT) {
			_, err := t.fetcher.FetchReader("some-buffer")
			Expect(t, err != nil).To(Equal(true))
		})
	})
}

func TestIOFetcherFetchWriter(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) (*testing.T, *iofetcher.IOFetcher) {
		return t, iofetcher.New()
	})

	o.Group("when Create() has been called", func() {
		o.BeforeEach(func(t *testing.T, fetcher *iofetcher.IOFetcher) (*testing.T, *iofetcher.IOFetcher) {
			err := fetcher.Create("some-buffer")
			Expect(t, err == nil).To(Equal(true))

			return t, fetcher
		})

		o.Spec("it returns the same writer each time", func(t *testing.T, fetcher *iofetcher.IOFetcher) {
			writerA, err := fetcher.FetchWriter("some-buffer")
			Expect(t, err == nil).To(Equal(true))

			writerB, err := fetcher.FetchWriter("some-buffer")
			Expect(t, err == nil).To(Equal(true))

			Expect(t, writerA != nil).To(Equal(true))
			Expect(t, writerA).To(Equal(writerB))
		})
	})

	o.Group("when Create() has not been called", func() {
		o.Spec("it returns an error", func(t *testing.T, fetcher *iofetcher.IOFetcher) {
			_, err := fetcher.FetchWriter("some-buffer")
			Expect(t, err != nil).To(Equal(true))
		})
	})
}
