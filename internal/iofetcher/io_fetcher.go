package iofetcher

import (
	"fmt"
	"log"

	"github.com/apoydence/talaria/internal/readers/ringbuffer"
	"github.com/apoydence/talaria/internal/server"
)

type IOFetcher struct {
	bufs map[string]*ringbuffer.RingBuffer
}

func New() *IOFetcher {
	return &IOFetcher{
		bufs: make(map[string]*ringbuffer.RingBuffer),
	}
}

func (f *IOFetcher) Create(name string) error {
	log.Printf("Creating '%s'", name)
	if _, ok := f.bufs[name]; ok {
		log.Printf("'%s' already exists...", name)
		return nil
	}

	f.bufs[name] = ringbuffer.New(100)
	return nil
}

func (f *IOFetcher) FetchWriter(name string) (server.Writer, error) {
	writer, ok := f.bufs[name]
	if !ok {
		return nil, fmt.Errorf("'%s' must be created before being fetched", name)
	}

	return writer, nil
}

func (f *IOFetcher) FetchReader(name string) (server.Reader, error) {
	reader, ok := f.bufs[name]
	if !ok {
		return nil, fmt.Errorf("'%s' must be created before being fetched", name)
	}

	return reader, nil
}
