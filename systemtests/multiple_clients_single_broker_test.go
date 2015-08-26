package systemtests_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var _ = Describe("MultipleClientsSingleBroker", func() {
	var (
		URL     string
		session *gexec.Session
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "systemtalaria")
		Expect(err).ToNot(HaveOccurred())
		URL, session = startTalaria(tmpDir)
	})

	AfterEach(func() {
		session.Kill()
		session.Wait("10s", "100ms")

		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("Writes and reads from separate files", func(done Done) {
		defer close(done)
		wg := sync.WaitGroup{}
		defer wg.Wait()

		runTest := func(name string) {
			defer wg.Done()
			connection := startConnection(URL)
			fileId, err := connection.FetchFile(name)
			Expect(err).ToNot(HaveOccurred())
			for i := byte(0); i < 100; i++ {
				_, err = connection.WriteToFile(fileId, []byte{i})
				Expect(err).ToNot(HaveOccurred())
			}

			data, err := connection.ReadFromFile(fileId)
			Expect(err).ToNot(HaveOccurred())
			for i := 0; i < 100; i++ {
				Expect(data[i]).To(BeEquivalentTo(i))
			}
		}

		count := 10
		wg.Add(count)
		for i := 0; i < count; i++ {
			go func(num int) {
				defer GinkgoRecover()
				runTest(fmt.Sprintf("some-file-%d", num))
			}(i)
		}
	}, 10)

	It("Writes and reads from the same file", func(done Done) {
		defer close(done)
		wg := sync.WaitGroup{}
		defer wg.Wait()
		count := 2
		wg.Add(count)

		runTest := func(value byte) {
			defer wg.Done()
			connectionW := startConnection(URL)
			connectionR := startConnection(URL)
			fileIdW, err := connectionW.FetchFile("some-name")
			Expect(err).ToNot(HaveOccurred())
			fileIdR, err := connectionR.FetchFile("some-name")
			Expect(err).ToNot(HaveOccurred())
			go func() {
				defer GinkgoRecover()
				for i := 0; i < 100; i++ {
					_, err = connectionW.WriteToFile(fileIdW, []byte{value})
					Expect(err).ToNot(HaveOccurred())
					time.Sleep(time.Millisecond)
				}
			}()

			valueCount := 0
			for i := 0; i < count*100; {
				data, err := connectionR.ReadFromFile(fileIdR)
				Expect(err).ToNot(HaveOccurred())
				i += len(data)
				for _, d := range data {
					if d == value {
						valueCount++
					}
				}
			}
			Expect(valueCount).To(Equal(100))
		}

		for i := 0; i < count; i++ {
			go func(num byte) {
				defer GinkgoRecover()
				runTest(num)
			}(byte(i))
		}
	}, 10)

})
