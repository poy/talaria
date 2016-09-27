package end2end_test

import (
	"fmt"
	"os/exec"
	"time"

	"google.golang.org/grpc"

	"github.com/apoydence/talaria/internal/end2end"
	"github.com/apoydence/talaria/pb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"testing"
)

func TestEnd2end(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "End2end Suite")
}

var (
	talariaPort    int
	talariaSession *gexec.Session
	clientConn     *grpc.ClientConn
	talariaClient  pb.TalariaClient
)

var _ = BeforeSuite(func() {
	startTalariaNode()
	talariaClient = connectToTalariaNode()
})

var _ = AfterSuite(func() {
	if clientConn != nil {
		clientConn.Close()
	}

	talariaSession.Kill().Wait(5 * time.Second)
	gexec.CleanupBuildArtifacts()
})

func connectToTalariaNode() pb.TalariaClient {
	var err error
	clientConn, err = grpc.Dial(fmt.Sprintf("localhost:%d", talariaPort), grpc.WithInsecure())
	Expect(err).ToNot(HaveOccurred())

	return pb.NewTalariaClient(clientConn)
}

func startTalariaNode() {
	talariaPort = end2end.AvailablePort()
	path, err := gexec.Build("github.com/apoydence/talaria/node")
	Expect(err).ToNot(HaveOccurred())
	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("PORT=%d", talariaPort),
	}
	talariaSession, err = gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())
	Consistently(talariaSession.Exited).ShouldNot(BeClosed())
}
