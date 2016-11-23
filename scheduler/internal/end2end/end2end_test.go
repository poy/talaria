package end2end_test

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
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
	schedulerProcess *os.Process
	schedulerClient  pb.SchedulerClient
	intraPort        int
	mockServer       *mockIntraServer
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

		intraPort, mockServer := startMockIntraServer()
		schedulerPort, schedulerProcess := startScheduler(intraPort)

		schedulerClient, closer := connectToScheduler(schedulerPort)

		testhelpers.AlwaysReturn(mockServer.CreateOutput.Ret0, new(intra.CreateResponse))
		close(mockServer.CreateOutput.Ret1)

		return TT{
			T:                t,
			createInfo:       createInfo,
			schedulerProcess: schedulerProcess,
			schedulerClient:  schedulerClient,
			intraPort:        intraPort,
			mockServer:       mockServer,
			closer:           closer,
		}
	})

	o.AfterEach(func(t TT) {
		t.closer.Close()
		t.schedulerProcess.Kill()
		t.schedulerProcess.Wait()
	})

	o.Spec("it selects Node to create buffer via intra API", func(t TT) {
		var resp *pb.CreateResponse
		f := func() bool {
			var err error
			resp, err = t.schedulerClient.Create(context.Background(), t.createInfo)
			return err == nil
		}

		Expect(t, f).To(ViaPolling(BeTrue()))
		expected := &intra.CreateInfo{
			Name: t.createInfo.Name,
		}

		Expect(t, resp.Uri).To(Equal(fmt.Sprintf("localhost:%d", t.intraPort)))
		Expect(t, t.mockServer.CreateInput.Arg1).To(ViaPolling(
			Chain(Receive(), Equal(expected)),
		))
	})
}

// var _ = Describe("End2end", func() {
// 	Describe("Create()", func() {
// 		var (
// 			createInfo *pb.CreateInfo
// 		)

// 		BeforeEach(func() {
// 			createInfo = &pb.CreateInfo{
// 				Name: createName(),
// 			}
// 		})

// 		JustBeforeEach(func() {
// 			testhelpers.AlwaysReturn(mockServer.CreateOutput.Ret0, new(intra.CreateResponse))
// 			close(mockServer.CreateOutput.Ret1)
// 		})

// 		Context("Create() doesn't return an error", func() {
// 			It("selects Node to create buffer via intra API", func() {
// 				resp, err := schedulerClient.Create(context.Background(), createInfo)
// 				Expect(err).ToNot(HaveOccurred())
// 				expected := &intra.CreateInfo{
// 					Name: createInfo.Name,
// 				}

// 				Expect(resp.Uri).To(Equal(fmt.Sprintf("localhost:%d", intraPort)))
// 				Eventually(mockServer.CreateInput.Arg1).Should(BeCalled(With(expected)))
// 			})
// 		})
// 	})
// })

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

func startScheduler(intraPort int) (int, *os.Process) {
	schedulerPort := end2end.AvailablePort()

	path, err := gexec.Build("github.com/apoydence/talaria/scheduler")
	if err != nil {
		panic(err)
	}
	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("PORT=%d", schedulerPort),
		fmt.Sprintf("NODES=localhost:%d", intraPort),
	}

	if err := command.Start(); err != nil {
		panic(err)
	}

	return schedulerPort, command.Process
}

func startMockIntraServer() (int, *mockIntraServer) {
	intraPort := end2end.AvailablePort()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", intraPort))
	if err != nil {
		panic(err)
	}

	mockServer := newMockIntraServer()

	grpcServer := grpc.NewServer()

	intra.RegisterNodeServer(grpcServer, mockServer)
	go grpcServer.Serve(lis)

	return intraPort, mockServer
}
