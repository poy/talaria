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
		session *gexec.Session
		client  *broker.Client
		URL     string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "systemtalaria")
		Expect(err).ToNot(HaveOccurred())
		URL, session = startTalaria(tmpDir)
		client = startClient(URL)
	})

	AfterEach(func() {
		session.Kill()
		session.Wait("10s", "100ms")

		Expect(os.RemoveAll(tmpDir)).To(Succeed())
		client.Close()
	})

	It("Writes and reads from a single file", func(done Done) {
		defer close(done)
		fileId, err := client.FetchFile("some-file")
		Expect(err).ToNot(HaveOccurred())
		for i := byte(0); i < 100; i++ {
			_, err = client.WriteToFile(fileId, []byte{i})
			Expect(err).ToNot(HaveOccurred())
		}

		data, err := client.ReadFromFile(fileId)
		Expect(err).ToNot(HaveOccurred())
		for i := 0; i < 100; i++ {
			Expect(data[i]).To(BeEquivalentTo(i))
		}
	}, 5)

	It("Writes and reads from a single file at the same time", func(done Done) {
		defer close(done)
		clientW := startClient(URL)
		clientR := startClient(URL)
		fileIdW, err := clientW.FetchFile("some-file")
		Expect(err).ToNot(HaveOccurred())
		fileIdR, err := clientR.FetchFile("some-file")
		Expect(err).ToNot(HaveOccurred())

		var wg sync.WaitGroup
		defer wg.Wait()
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, err = clientW.WriteToFile(fileIdW, []byte{byte(i)})
				Expect(err).ToNot(HaveOccurred())
				time.Sleep(time.Millisecond)
			}
		}()

		var result []byte
		for len(result) < 10 {
			data, err := clientR.ReadFromFile(fileIdR)
			Expect(err).ToNot(HaveOccurred())
			result = append(result, data...)
		}

		for i := 0; i < 10; i++ {
			Expect(result[i]).To(BeEquivalentTo(i))
		}
	}, 5)

})
