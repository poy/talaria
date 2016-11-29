package end2end_test

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/internal/end2end"
	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/onsi/gomega/gexec"
)

type TT struct {
	*testing.T
	createInfo       *pb.CreateInfo
	leaderInfo       *intra.LeaderInfo
	leaderRequest    *intra.LeaderRequest
	schedulerProcess *os.Process
	schedulerClient  pb.SchedulerClient
	intraPorts       []int
	mockServers      []*mockIntraServer
	closer           io.Closer
}

func TestSchedulerEnd2End(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TT {
		createInfo := &pb.CreateInfo{
			Name: createName(),
		}

		leaderRequest := &intra.LeaderRequest{
			Name: createInfo.Name,
		}

		leaderInfo := &intra.LeaderInfo{
			Peer: &intra.PeerInfo{
				Uri: "some-uri",
				Id:  99,
			},
		}

		intraPorts, mockServers := startMockIntraServer(3)
		schedulerPort, schedulerProcess := startScheduler(intraPorts...)

		schedulerClient, closer := connectToScheduler(schedulerPort)

		for _, mockServer := range mockServers {
			testhelpers.AlwaysReturn(mockServer.CreateOutput.Ret0, new(intra.CreateResponse))
			close(mockServer.CreateOutput.Ret1)

			mockServer.LeaderOutput.Ret0 <- leaderInfo
			close(mockServer.LeaderOutput.Ret1)
		}

		return TT{
			T:                t,
			createInfo:       createInfo,
			leaderInfo:       leaderInfo,
			leaderRequest:    leaderRequest,
			schedulerProcess: schedulerProcess,
			schedulerClient:  schedulerClient,
			intraPorts:       intraPorts,
			mockServers:      mockServers,
			closer:           closer,
		}
	})

	o.AfterEach(func(t TT) {
		t.closer.Close()
		t.schedulerProcess.Kill()
		t.schedulerProcess.Wait()
	})

	o.Spec("it selects 3 Nodes to create a buffer via intra API", func(t TT) {
		var resp *pb.CreateResponse
		f := func() bool {
			var err error
			resp, err = t.schedulerClient.Create(context.Background(), t.createInfo)
			return err == nil
		}

		Expect(t, f).To(ViaPolling(BeTrue()))
		Expect(t, resp.Uri).To(Equal(t.leaderInfo.Peer.Uri))

		expected := &intra.CreateInfo{
			Name: t.createInfo.Name,
		}
		for _, mockServer := range t.mockServers {
			Expect(t, mockServer.CreateInput.In).To(ViaPolling(
				Chain(Receive(), Equal(expected)),
			))
		}
	})
}

func createName() string {
	return fmt.Sprintf("some-buffer-%d", rand.Int63())
}

func connectToScheduler(schedulerPort int) (pb.SchedulerClient, io.Closer) {
	clientConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", schedulerPort), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return pb.NewSchedulerClient(clientConn), clientConn
}

func startScheduler(intraPorts ...int) (int, *os.Process) {
	schedulerPort := end2end.AvailablePort()

	path, err := gexec.Build("github.com/apoydence/talaria/scheduler")
	if err != nil {
		panic(err)
	}
	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("PORT=%d", schedulerPort),
		fmt.Sprintf("NODES=%s", buildNodeURIs(intraPorts)),
	}

	if err := command.Start(); err != nil {
		panic(err)
	}

	return schedulerPort, command.Process
}

func startMockIntraServer(count int) ([]int, []*mockIntraServer) {
	var (
		ports       []int
		mockServers []*mockIntraServer
	)

	for i := 0; i < count; i++ {
		intraPort := end2end.AvailablePort()
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", intraPort))
		if err != nil {
			panic(err)
		}

		mockServer := newMockIntraServer()

		grpcServer := grpc.NewServer()

		intra.RegisterNodeServer(grpcServer, mockServer)
		go grpcServer.Serve(lis)

		ports = append(ports, intraPort)
		mockServers = append(mockServers, mockServer)
	}

	return ports, mockServers
}

func buildNodeURIs(ports []int) string {
	var URIs []string
	for _, port := range ports {
		URIs = append(URIs, fmt.Sprintf("localhost:%d", port))
	}
	return strings.Join(URIs, ",")
}
