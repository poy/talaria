package systemtests_test

import (
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/apoydence/talaria/broker"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var _ = Describe("SingleConnectionSingleBroker", func() {

	var (
		session    *gexec.Session
		connection *broker.Connection
		URL        string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "systemtalaria")
		Expect(err).ToNot(HaveOccurred())
		URL, session = startTalaria(tmpDir)
		connection = startConnection(URL)
	})

	AfterEach(func() {
		session.Kill()
		session.Wait("10s", "100ms")

		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("Writes and reads from a single file", func() {
		fileId, err := connection.FetchFile("some-file")
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
	})

	It("Writes and reads from a single file at the same time", func(done Done) {
		defer close(done)
		connectionW := startConnection(URL)
		connectionR := startConnection(URL)
		fileIdW, err := connectionW.FetchFile("some-file")
		Expect(err).ToNot(HaveOccurred())
		fileIdR, err := connectionR.FetchFile("some-file")
		Expect(err).ToNot(HaveOccurred())

		wg := sync.WaitGroup{}
		defer wg.Wait()
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, err = connectionW.WriteToFile(fileIdW, []byte{byte(i)})
				Expect(err).ToNot(HaveOccurred())
				time.Sleep(time.Millisecond)
			}
		}()

		var result []byte
		for len(result) < 10 {
			data, err := connectionR.ReadFromFile(fileIdR)
			Expect(err).ToNot(HaveOccurred())
			result = append(result, data...)
		}

		for i := 0; i < 10; i++ {
			Expect(result[i]).To(BeEquivalentTo(i))
		}
	}, 5)

})
