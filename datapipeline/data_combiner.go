package datapipeline

import (
	"io"
	"sync"
)

type DataCombiner struct {
	writer    io.WriteCloser
	closeOnce *sync.Once

	stopChan chan struct{}
	dataChan chan []byte
	errChan  chan error
}

func NewDataCombiner(writer io.WriteCloser) *DataCombiner {
	d := &DataCombiner{
		writer:    writer,
		closeOnce: &sync.Once{},
		stopChan:  make(chan struct{}),
		dataChan:  make(chan []byte),
		errChan:   make(chan error, 1),
	}
	go d.run()
	return d
}

func (d *DataCombiner) Write(data []byte) (int, error) {
	select {
	case err := <-d.errChan:
		return -1, err
	default:
	}
	d.dataChan <- data
	return len(data), nil
}

func (d *DataCombiner) Close() error {
	defer close(d.stopChan)
	d.setError(io.EOF)
	return d.writer.Close()
}

func (d *DataCombiner) setError(err error) {
	if err != nil {
		d.errChan <- err
	}
}

func (d *DataCombiner) run() {
	for {
		select {
		case <-d.stopChan:
			return
		case data, ok := <-d.dataChan:
			if !ok {
				return
			}
			_, err := d.writer.Write(data)
			d.setError(err)
		}
	}
}
