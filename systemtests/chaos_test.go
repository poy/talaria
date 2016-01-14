package systemtests_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var _ = Describe("Chaos", func() {
	var (
		client   *broker.Client
		brokers  []string
		sessions []*gexec.Session
		tmpDirs  []string
	)

	JustBeforeEach(func() {
		client = startClient(brokers...)
	})

	AfterEach(func() {
		for _, session := range sessions {
			session.Kill()
		}

		for _, tmpDir := range tmpDirs {
			Expect(os.RemoveAll(tmpDir)).To(Succeed())
		}
		client.Close()
	})

	var startBroker = func() {
		tmpDir, err := ioutil.TempDir("/tmp", "systemtalaria")
		Expect(err).ToNot(HaveOccurred())
		uri, session := startTalaria(tmpDir)

		brokers = append(brokers, uri)
		sessions = append(sessions, session)
		tmpDirs = append(tmpDirs, tmpDir)
	}

	Context("4 brokers", func() {

		BeforeEach(func() {
			for i := 0; i < 4; i++ {
				startBroker()
			}
		})

		var findSession = func(uri string) *gexec.Session {
			for i, broker := range brokers {
				if broker == uri {
					return sessions[i]
				}
			}
			return nil
		}

		var killLeader = func(fileName string) {
			meta, err := client.FileMeta(fileName)
			Expect(err).ToNot(HaveOccurred())
			Expect(meta.GetReplicaURIs()).To(HaveLen(1))
			leaderUri := meta.GetReplicaURIs()[0]

			leader := findSession(leaderUri)
			Expect(leader).ToNot(BeNil())

			By(fmt.Sprintf("killing leader %s...", leaderUri))
			leader.Kill()
			time.Sleep(5 * time.Second)
			By(fmt.Sprintf("done killing leader %s", leaderUri))
		}

		It("continues to write to a file once the leader is killed", func(done Done) {
			defer close(done)
			fileName := "some-file"

			Expect(client.CreateFile(fileName)).To(Succeed())
			writer, err := client.FetchWriter(fileName)
			Expect(err).ToNot(HaveOccurred())

			for i := byte(0); i < 50; i++ {
				_, err := writer.WriteToFile([]byte{i})
				Expect(err).ToNot(HaveOccurred())
			}

			killLeader(fileName)

			for i := byte(50); i < 100; i++ {
				_, err := writer.WriteToFile([]byte{i})
				Expect(err).ToNot(HaveOccurred())
			}

			killLeader(fileName)

			for i := byte(100); i < 150; i++ {
				_, err := writer.WriteToFile([]byte{i})
				Expect(err).ToNot(HaveOccurred())
			}
		}, 300)
	})

})
