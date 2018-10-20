package end2end_test

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"golang.org/x/net/context"

	"github.com/poy/eachers/testhelpers"
	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/talaria/api/intra"
	pb "github.com/poy/talaria/api/v1"
	"github.com/poy/talaria/internal/end2end"
	"github.com/onsi/gomega/gexec"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		grpclog.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags))
	}

	os.Exit(m.Run())
}

type TT struct {
	*testing.T
	createInfo       *pb.CreateInfo
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
			Name:       createName(),
			BufferSize: 909,
		}

		leaderRequest := &intra.LeaderRequest{
			Name: createInfo.Name,
		}

		intraPorts, mockServers := startMockIntraServer(3)
		schedulerPort, schedulerProcess := startScheduler(intraPorts...)

		schedulerClient, closer := connectToScheduler(schedulerPort)

		for i, mockServer := range mockServers {
			testhelpers.AlwaysReturn(mockServer.CreateOutput.Ret0, new(intra.CreateResponse))
			close(mockServer.CreateOutput.Ret1)

			testhelpers.AlwaysReturn(mockServer.LeaderOutput.Ret0, &intra.LeaderResponse{
				Addr: fmt.Sprintf("127.0.0.1:%d", intraPorts[i]),
			})
			close(mockServer.LeaderOutput.Ret1)
		}

		return TT{
			T:                t,
			createInfo:       createInfo,
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

	o.Group("when the node is reporting standard status", func() {
		o.BeforeEach(func(t TT) TT {
			for i, mockServer := range t.mockServers {
				testhelpers.AlwaysReturn(mockServer.StatusOutput.Ret0, &intra.StatusResponse{
					ExternalAddr: fmt.Sprintf("127.0.0.1:%d", t.intraPorts[i]),
					Buffers: []*intra.StatusBufferInfo{{
						Name: t.createInfo.Name,
					}},
				})
				close(mockServer.StatusOutput.Ret1)

				testhelpers.AlwaysReturn(t.mockServers[i].UpdateConfigOutput.Ret0, &intra.UpdateConfigResponse{})
				close(t.mockServers[i].UpdateConfigOutput.Ret1)
			}

			return t
		})

		o.Spec("it selects 3 Nodes to create a buffer via intra API", func(t TT) {
			f := func() bool {
				_, err := t.schedulerClient.Create(context.Background(), t.createInfo)
				return err == nil
			}

			Expect(t, f).To(ViaPolling(BeTrue()))

			var info *intra.CreateInfo
			for _, mockServer := range t.mockServers {
				Expect(t, mockServer.CreateInput.In).To(ViaPolling(
					Chain(Receive(), Fetch(&info)),
				))
				Expect(t, info.Name).To(Equal(t.createInfo.Name))
				Expect(t, info.BufferSize).To(Equal(uint64(909)))
				Expect(t, info.Peers).To(Contain([]interface{}{
					&intra.PeerInfo{Addr: fmt.Sprintf("127.0.0.1:%d", t.intraPorts[0])},
					&intra.PeerInfo{Addr: fmt.Sprintf("127.0.0.1:%d", t.intraPorts[1])},
					&intra.PeerInfo{Addr: fmt.Sprintf("127.0.0.1:%d", t.intraPorts[2])},
				}...))
			}
		})
	})

	o.Group("when a buffer does not have 3 nodes", func() {
		o.BeforeEach(func(t TT) TT {
			testhelpers.AlwaysReturn(t.mockServers[0].StatusOutput.Ret0, &intra.StatusResponse{
				ExternalAddr: fmt.Sprintf("some-external-%d", 0),
				Buffers: []*intra.StatusBufferInfo{
					{Name: "standalone", ExpectedNodes: []string{fmt.Sprintf("127.0.0.1:%d", t.intraPorts[0])}},
				},
			})
			close(t.mockServers[0].StatusOutput.Ret1)

			testhelpers.AlwaysReturn(t.mockServers[0].UpdateConfigOutput.Ret0, &intra.UpdateConfigResponse{})
			close(t.mockServers[0].UpdateConfigOutput.Ret1)

			for i, m := range t.mockServers[1:] {
				testhelpers.AlwaysReturn(m.StatusOutput.Ret0, &intra.StatusResponse{
					ExternalAddr: fmt.Sprintf("some-external-%d", i),
				})
				close(m.StatusOutput.Ret1)

				testhelpers.AlwaysReturn(m.UpdateConfigOutput.Ret0, &intra.UpdateConfigResponse{})
				close(m.UpdateConfigOutput.Ret1)
			}

			return t
		})

		o.Spec("it adds a node to the buffer", func(t TT) {
			combine := make(chan string, 100)
			go func() {
				select {
				case c := <-t.mockServers[1].CreateInput.In:
					combine <- c.Name
				case c := <-t.mockServers[2].CreateInput.In:
					combine <- c.Name
				case <-t.mockServers[0].CreateInput.In:
					panic("Should not be invoked")
				}
			}()

			Expect(t, combine).To(ViaPollingMatcher{
				Duration: 3 * time.Second,
				Matcher:  Chain(Receive(), Equal("standalone")),
			})

			var req *intra.UpdateConfigRequest
			Expect(t, t.mockServers[0].UpdateConfigInput.In).To(ViaPolling(
				Chain(Receive(), Fetch(&req)),
			))

			Expect(t, req.Name).To(Equal("standalone"))
			Expect(t, req.ExpectedNodes).To(Or(
				Contain(fmt.Sprintf("127.0.0.1:%d", t.intraPorts[1])),
				Contain(fmt.Sprintf("127.0.0.1:%d", t.intraPorts[2])),
			))
		})

		o.Spec("it lists the buffers", func(t TT) {
			var resp *pb.ListResponse
			f := func() int {
				var err error
				resp, err = t.schedulerClient.ListClusterInfo(context.Background(), new(pb.ListInfo))
				if err != nil {
					return 0
				}

				return len(resp.Info)
			}
			Expect(t, f).To(ViaPollingMatcher{
				Duration: 3 * time.Second,
				Matcher:  Equal(1),
			})

			Expect(t, resp.Info[0].Name).To(Equal("standalone"))
			Expect(t, resp.Info[0].Leader).To(Equal("some-external-0"))
			Expect(t, resp.Info[0].Nodes).To(Contain(
				&pb.NodeInfo{URI: fmt.Sprintf("127.0.0.1:%d", t.intraPorts[0])},
			))
		})

	})
}

func createName() string {
	return fmt.Sprintf("some-buffer-%d", rand.Int63())
}

func connectToScheduler(schedulerPort int) (pb.SchedulerClient, io.Closer) {
	clientConn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", schedulerPort), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return pb.NewSchedulerClient(clientConn), clientConn
}

func startScheduler(intraPorts ...int) (int, *os.Process) {
	schedulerPort := end2end.AvailablePort()

	path, err := gexec.Build("github.com/poy/talaria/scheduler")
	if err != nil {
		panic(err)
	}
	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("ADDR=127.0.0.1:%d", schedulerPort),
		fmt.Sprintf("NODES=%s", buildNodeURIs(intraPorts)),
	}

	if testing.Verbose() {
		command.Stderr = os.Stderr
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
		lis, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", intraPort))
		if err != nil {
			panic(err)
		}
		log.Printf("Mock Node started at %s", lis.Addr().String())

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
		URIs = append(URIs, fmt.Sprintf("127.0.0.1:%d", port))
	}
	return strings.Join(URIs, ",")
}
