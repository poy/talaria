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
		name := "some-file"
		err := client.FetchFile(name)
		Expect(err).ToNot(HaveOccurred())
		for i := byte(0); i < 100; i++ {
			_, err = client.WriteToFile(name, []byte{i})
			Expect(err).ToNot(HaveOccurred())
		}

		for i := 0; i < 100; i++ {
			data, index, err := client.ReadFromFile(name)
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(HaveLen(1))
			Expect(data[0]).To(BeEquivalentTo(i))
			Expect(index).To(BeEquivalentTo(i))
		}
	}, 5)

	It("Writes and reads from a single file at the same time", func(done Done) {
		defer close(done)
		name := "some-file"
		clientW := startClient(URL)
		clientR := startClient(URL)
		err := clientW.FetchFile(name)
		Expect(err).ToNot(HaveOccurred())
		err = clientR.FetchFile(name)
		Expect(err).ToNot(HaveOccurred())

		var wg sync.WaitGroup
		defer wg.Wait()
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, err = clientW.WriteToFile(name, []byte{byte(i)})
				Expect(err).ToNot(HaveOccurred())
				time.Sleep(time.Millisecond)
			}
		}()

		var result []byte
		for len(result) < 10 {
			data, _, err := clientR.ReadFromFile(name)
			Expect(err).ToNot(HaveOccurred())
			result = append(result, data...)
		}

		for i := 0; i < 10; i++ {
			Expect(result[i]).To(BeEquivalentTo(i))
		}
	}, 5)

})
