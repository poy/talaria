package systemtests_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/kvstore"
	"github.com/hashicorp/consul/api"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"testing"
)

func TestSystemtests(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Systemtests Suite")
}

var (
	path            string
	tmpDir          string
	nextTalariaPort int
	nextHealthPort  int

	consulTmpDir  string
	consulSession *gexec.Session
	consulClient  *api.Client
)

var _ = BeforeSuite(func() {
	nextTalariaPort = 8888
	nextHealthPort = 9999
	var err error
	path, err = gexec.Build("github.com/apoydence/talaria")
	Expect(err).ToNot(HaveOccurred())
	startConsul()
})

var _ = AfterSuite(func() {
	consulSession.Kill()
	consulSession.Wait("60s", "200ms")
	Expect(os.RemoveAll(consulTmpDir)).To(Succeed())
	gexec.CleanupBuildArtifacts()
})

var _ = BeforeEach(func() {
	sessions, _, err := consulClient.Session().List(nil)
	Expect(err).ToNot(HaveOccurred())
	for _, s := range sessions {
		keys, _, err := consulClient.KV().Keys("", "", nil)
		for _, k := range keys {
			pair := &api.KVPair{
				Key:     k,
				Session: s.ID,
			}
			consulClient.KV().Release(pair, nil)
		}

		_, err = consulClient.Session().Destroy(s.ID, nil)
		Expect(err).ToNot(HaveOccurred())
	}

	err = consulClient.Agent().CheckDeregister(kvstore.CheckName)
	Expect(err).ToNot(HaveOccurred())
	_, err = consulClient.KV().DeleteTree(kvstore.Prefix, nil)
	Expect(err).ToNot(HaveOccurred())
	_, err = consulClient.KV().DeleteTree(kvstore.AnnouncePrefix, nil)
	Expect(err).ToNot(HaveOccurred())
})

func startTalaria(tmpDir string) (string, *gexec.Session) {
	cmd := exec.Command(path, "-d", tmpDir, "-logLevel", "CRITICAL", "-port", fmt.Sprintf("%d", nextTalariaPort), "-healthPort", fmt.Sprintf("%d", nextHealthPort))
	session, err := gexec.Start(cmd, os.Stdout, os.Stdout)
	Expect(err).ToNot(HaveOccurred())
	Consistently(session.Exited, 1).ShouldNot(BeClosed())
	URL := fmt.Sprintf("ws://localhost:%d", nextTalariaPort)
	nextTalariaPort++
	nextHealthPort++
	return URL, session
}

func startClient(URLs ...string) *broker.Client {
	var client *broker.Client
	f := func() error {
		var err error
		client, err = broker.NewClient(URLs...)
		return err
	}
	Eventually(f, 5).ShouldNot(HaveOccurred())
	return client
}

func startConsul() {
	consulPath, err := gexec.Build("github.com/hashicorp/consul")
	Expect(err).ToNot(HaveOccurred())

	consulTmpDir, err = ioutil.TempDir("", "consul")
	Expect(err).ToNot(HaveOccurred())

	consulCmd := exec.Command(consulPath, "agent", "-server", "-bootstrap-expect", "1", "-data-dir", consulTmpDir, "-bind", "127.0.0.1")
	consulSession, err = gexec.Start(consulCmd, nil, nil)
	Expect(err).ToNot(HaveOccurred())
	Consistently(consulSession).ShouldNot(gexec.Exit())

	consulClient, err = api.NewClient(api.DefaultConfig())
	Expect(err).ToNot(HaveOccurred())

	f := func() error {
		_, _, err := consulClient.Catalog().Nodes(nil)
		return err
	}
	Eventually(f, 10).Should(BeNil())
}
