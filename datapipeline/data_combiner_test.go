package datapipeline_test

import (
	"fmt"
	"github.com/apoydence/talaria/datapipeline"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DataCombiner", func() {
	var (
		buffer       []byte
		mockWriter   *mockFile
		dataCombiner *datapipeline.DataCombiner
	)

	BeforeEach(func() {
		buffer = make([]byte, 1024)
		mockWriter = newMockFile(buffer, 0, make(chan int, 1))
		dataCombiner = datapipeline.NewDataCombiner(mockWriter)
	})

	It("Should take data from multiple producers and combines the data", func(done Done) {
		defer close(done)
		wg := &sync.WaitGroup{}
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(index int) {
				defer GinkgoRecover()
				defer wg.Done()
				data := []byte{}
				for j := 0; j < 20; j++ {
					data = append(data, byte(j+(index*20)))
				}
				n, err := dataCombiner.Write(data)
				Expect(n).To(Equal(len(data)))
				Expect(err).ToNot(HaveOccurred())
			}(i)
		}
		wg.Wait()

		for i := 0; i < 100; i++ {
			Expect(buffer).To(ContainElement(byte(i)))
		}
	})

	It("Should return an error if the write fails", func(done Done) {
		defer close(done)
		mockWriter.ErrorChan <- fmt.Errorf("something")
		errChan := make(chan error, 1)
		for {
			if _, err := dataCombiner.Write([]byte{}); err != nil {
				errChan <- err
				break
			}
		}
		Expect(errChan).To(Receive())
	})

	It("Should not panic when it is closed and then written to", func(done Done) {
		defer close(done)
		err := dataCombiner.Close()
		Expect(err).ToNot(HaveOccurred())
		Expect(func() { dataCombiner.Write([]byte{}) }).ToNot(Panic())
	})
})
