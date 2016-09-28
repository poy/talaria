package end2end_test

import (
	"fmt"
	"os/exec"
	"time"

	"github.com/apoydence/talaria/internal/end2end"
	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"google.golang.org/grpc"

	"testing"
)

func TestEnd2end(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "End2end Suite")
}

var (
	nodePort int

	sessions    []*gexec.Session
	clientConns []*grpc.ClientConn

	nodeClient      pb.TalariaClient
	intraNodeClient intra.NodeClient
)

var _ = BeforeSuite(func() {
	startNode()
	nodeClient = connectToNode()
	intraNodeClient = connectToIntraNode()
})

var _ = AfterSuite(func() {
	for _, clientConn := range clientConns {
		clientConn.Close()
	}

	for _, session := range sessions {
		session.Kill().Wait(5 * time.Second)
	}

	gexec.CleanupBuildArtifacts()
})

func connectToNode() pb.TalariaClient {
	clientConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", nodePort), grpc.WithInsecure())
	Expect(err).ToNot(HaveOccurred())
	clientConns = append(clientConns, clientConn)

	return pb.NewTalariaClient(clientConn)
}

func connectToIntraNode() intra.NodeClient {
	clientConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", nodePort), grpc.WithInsecure())
	Expect(err).ToNot(HaveOccurred())
	clientConns = append(clientConns, clientConn)

	return intra.NewNodeClient(clientConn)
}

func startNode() {
	nodePort = end2end.AvailablePort()

	path, err := gexec.Build("github.com/apoydence/talaria/node")
	Expect(err).ToNot(HaveOccurred())
	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("PORT=%d", nodePort),
	}

	nodeSession, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())
	Consistently(nodeSession.Exited).ShouldNot(BeClosed())
	sessions = append(sessions, nodeSession)
}
