//go:generate hel
package end2end_test

import (
	"fmt"
	"net"
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
	mockServer    *mockIntraServer
	schedulerPort int
	intraPort     int

	sessions    []*gexec.Session
	clientConns []*grpc.ClientConn

	schedulerClient pb.SchedulerClient
)

var _ = BeforeSuite(func() {
	startMockIntraServer()
	startScheduler()
	schedulerClient = connectToScheduler()
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

func connectToScheduler() pb.SchedulerClient {
	clientConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", schedulerPort), grpc.WithInsecure())
	Expect(err).ToNot(HaveOccurred())
	clientConns = append(clientConns, clientConn)

	return pb.NewSchedulerClient(clientConn)
}

func startScheduler() {
	schedulerPort = end2end.AvailablePort()

	path, err := gexec.Build("github.com/apoydence/talaria/scheduler")
	Expect(err).ToNot(HaveOccurred())
	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("PORT=%d", schedulerPort),
		fmt.Sprintf("NODES=localhost:%d", intraPort),
	}

	schedulerSession, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())
	Consistently(schedulerSession.Exited).ShouldNot(BeClosed())
	sessions = append(sessions, schedulerSession)
}

func startMockIntraServer() {
	intraPort = end2end.AvailablePort()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", intraPort))
	Expect(err).ToNot(HaveOccurred())

	mockServer = newMockIntraServer()

	grpcServer := grpc.NewServer()

	intra.RegisterNodeServer(grpcServer, mockServer)
	go grpcServer.Serve(lis)
}
