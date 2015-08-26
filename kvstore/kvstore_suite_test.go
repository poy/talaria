package kvstore_test

import (
	"io/ioutil"
	"os"
	"os/exec"
	"testing"

	"github.com/apoydence/talaria/logging"
	"github.com/hashicorp/consul/api"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

func TestKvstore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kvstore Suite")
}

var (
	consulSession *gexec.Session
	tmpDir        string
	consulClient  *api.Client
)

var _ = BeforeSuite(func() {
	logging.SetLevel(logging.CRITICAL)
	consulPath, err := gexec.Build("github.com/hashicorp/consul")
	Expect(err).ToNot(HaveOccurred())

	tmpDir, err = ioutil.TempDir("", "consul")
	Expect(err).ToNot(HaveOccurred())

	consulCmd := exec.Command(consulPath, "agent", "-server", "-bootstrap-expect", "1", "-data-dir", tmpDir, "-bind", "127.0.0.1")
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
})

var _ = AfterSuite(func() {
	consulSession.Kill()
	consulSession.Wait("60s", "200ms")
	Expect(os.RemoveAll(tmpDir)).To(Succeed())
	gexec.CleanupBuildArtifacts()
})
