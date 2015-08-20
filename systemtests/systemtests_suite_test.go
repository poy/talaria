package systemtests_test

import (
	"os"
	"os/exec"

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
	path    string
	session *gexec.Session
	tmpDir  string
	URL     string
)

var _ = BeforeSuite(func() {
	var err error
	path, err = gexec.Build("github.com/apoydence/talaria")
	Expect(err).ToNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
})

func startTalaria(tmpDir string) string {
	var err error
	cmd := exec.Command(path, "-d", tmpDir, "-logLevel", "CRITICAL")
	session, err = gexec.Start(cmd, os.Stdout, os.Stdout)
	Expect(err).ToNot(HaveOccurred())
	Consistently(session.Exited, 1).ShouldNot(BeClosed())
	return "ws://localhost:8888"
}
