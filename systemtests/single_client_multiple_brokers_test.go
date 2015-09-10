package systemtests_test

import (
	"io/ioutil"
	"os"

	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var _ = Describe("SingleClientMultipleBrokers", func() {
	var (
		tmpDirs  []string
		sessions []*gexec.Session
		client   *broker.Client
	)

	BeforeEach(func() {
		var URLs []string
		var err error
		for i := 0; i < 3; i++ {
			tmpDir, err = ioutil.TempDir("/tmp", "systemtalaria")
			Expect(err).ToNot(HaveOccurred())
			tmpDirs = append(tmpDirs, tmpDir)

			URL, session := startTalaria(tmpDir)
			URLs = append(URLs, URL)
			sessions = append(sessions, session)
		}
		client = startClient(URLs...)
	})

	AfterEach(func() {
		for _, session := range sessions {
			session.Kill()
			session.Wait("10s", "100ms")
		}

		for _, tmpDir := range tmpDirs {
			Expect(os.RemoveAll(tmpDir)).To(Succeed())
		}

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

})
