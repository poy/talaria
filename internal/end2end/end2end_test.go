package end2end_test

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"golang.org/x/net/context"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	pb "github.com/apoydence/talaria/api/v1"
	"github.com/apoydence/talaria/internal/end2end"
	"github.com/onsi/gomega/gexec"
)

var (
	nodePorts      []int
	intraNodePorts []int
	schedulerPort  int
)

func setup() []*os.Process {
	schedulerPort = end2end.AvailablePort()
	nodePort1, intraNodePort1, nodeProcess1 := startNode()
	nodePort2, intraNodePort2, nodeProcess2 := startNode()
	nodePort3, intraNodePort3, nodeProcess3 := startNode()
	nodePorts = []int{nodePort1, nodePort2, nodePort3}
	intraNodePorts = []int{intraNodePort1, intraNodePort2, intraNodePort3}
	var schedulerProcess *os.Process
	schedulerProcess = startScheduler(schedulerPort, intraNodePorts)

	return []*os.Process{
		nodeProcess1,
		nodeProcess2,
		nodeProcess3,
		schedulerProcess,
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		grpclog.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags))
	}

	ps := setup()

	code := m.Run()

	for _, p := range ps {
		p.Kill()
		p.Wait()
	}

	os.Exit(code)
}

type TC struct {
	*testing.T
	bufferInfo      *pb.BufferInfo
	createInfo      *pb.CreateInfo
	nodeClient      pb.NodeClient
	schedulerClient pb.SchedulerClient
}

func TestEnd2EndBufferHasBeenCreated(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TC {
		nodeClients := setupNodeClients(intraNodePorts, nodePorts)
		schedulerClient := connectToScheduler(schedulerPort)

		bufferInfo := &pb.BufferInfo{
			Name: createName(),
		}

		createInfo := &pb.CreateInfo{
			Name: bufferInfo.Name,
		}

		var nodeClient pb.NodeClient
		f := func() bool {
			_, err := schedulerClient.Create(context.Background(), createInfo)
			return err == nil
		}
		Expect(t, f).To(ViaPollingMatcher{
			Matcher:  BeTrue(),
			Duration: 5 * time.Second,
		})

		f = func() bool {
			resp, err := schedulerClient.ListClusterInfo(context.Background(), &pb.ListInfo{
				Names: []string{createInfo.Name},
			})

			if err != nil {
				return false
			}

			if len(resp.Info) == 0 || resp.Info[0].Leader == "" {
				return false
			}

			nodeClient = fetchNodeClient(resp.Info[0].Leader, nodeClients)
			return true
		}

		Expect(t, f).To(ViaPollingMatcher{
			Matcher:  BeTrue(),
			Duration: 5 * time.Second,
		})

		return TC{
			T:               t,
			bufferInfo:      bufferInfo,
			createInfo:      createInfo,
			nodeClient:      nodeClient,
			schedulerClient: schedulerClient,
		}
	})

	o.Group("when tailing from beginning", func() {
		o.Spec("it writes data to a subscriber", func(t TC) {
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())
			writeTo(t.bufferInfo.Name, []byte("some-data-1"), writer)
			writeTo(t.bufferInfo.Name, []byte("some-data-2"), writer)

			data, _, _ := fetchReaderWithIndex(t.bufferInfo.Name, 0, true, t.nodeClient)
			Expect(t, data).To(ViaPolling(
				Chain(Receive(), Equal([]byte("some-data-1"))),
			))
			Expect(t, data).To(ViaPolling(
				Chain(Receive(), Equal([]byte("some-data-2"))),
			))
		})

		o.Spec("it reads what is available via Read", func(t TC) {
			_, _, errs := fetchReaderWithIndex(t.bufferInfo.Name, 0, false, t.nodeClient)

			Expect(t, errs).To(ViaPollingMatcher{
				Matcher:  Not(HaveLen(0)),
				Duration: 5 * time.Second,
			})
		})

		o.Spec("it tails via Read", func(t TC) {
			data, _, _ := fetchReaderWithIndex(t.bufferInfo.Name, 0, true, t.nodeClient)
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())

			writeSlowly(10, t.bufferInfo, writer).Wait()

			for i := 0; i < 10; i++ {
				expectedData := []byte(fmt.Sprintf("some-data-%d", i))
				Expect(t, data).To(ViaPollingMatcher{
					Matcher:  Chain(Receive(), Equal(expectedData)),
					Duration: 5 * time.Second,
				})
			}
		})

		o.Spec("it fails to write to a read only buffer", func(t TC) {
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())

			f := func() bool {
				_, err := t.schedulerClient.ReadOnly(context.Background(), &pb.ReadOnlyInfo{
					Name: t.bufferInfo.Name,
				})
				return err == nil
			}
			Expect(t, f).To(ViaPolling(BeTrue()))

			f = func() bool {
				err := writer.Send(&pb.WriteDataPacket{
					Name:    t.bufferInfo.Name,
					Message: []byte("some-data"),
				})
				return err == nil
			}

			Expect(t, f).To(ViaPolling(BeFalse()))
		})

		o.Group("when tailing from middle", func() {
			o.Spec("it reads from the given index", func(t TC) {
				writer, err := t.nodeClient.Write(context.Background())
				Expect(t, err == nil).To(BeTrue())
				writeTo(t.bufferInfo.Name, []byte("some-data-1"), writer)
				writeTo(t.bufferInfo.Name, []byte("some-data-2"), writer)
				writeTo(t.bufferInfo.Name, []byte("some-data-3"), writer)

				data, _, _ := fetchReaderWithIndex(t.bufferInfo.Name, 1, true, t.nodeClient)

				Expect(t, data).To(ViaPolling(
					Chain(Receive(), Equal([]byte("some-data-2"))),
				))
			})
		})

		o.Group("when tailing from end", func() {
			o.Spec("it reads from the given index", func(t TC) {
				writer, err := t.nodeClient.Write(context.Background())
				Expect(t, err == nil).To(BeTrue())
				writeTo(t.bufferInfo.Name, []byte("some-data-1"), writer)
				writeTo(t.bufferInfo.Name, []byte("some-data-2"), writer)
				writeTo(t.bufferInfo.Name, []byte("some-data-3"), writer)
				data, _, _ := fetchReaderWithIndex(t.bufferInfo.Name, 0, true, t.nodeClient)
				Expect(t, data).To(ViaPolling(HaveLen(3)))

				data, _ = fetchReaderLastIndex(t.bufferInfo.Name, t.nodeClient)

				Expect(t, data).To(ViaPolling(
					Chain(Receive(), Equal([]byte("some-data-3"))),
				))
			})
		})

	})
}

func TestEnd2EndBufferHasNotBeenCreated(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TC {
		nodeClients := setupNodeClients(intraNodePorts, nodePorts)
		nodeClient := fetchNodeClient(fmt.Sprintf("127.0.0.1:%d", nodePorts[0]), nodeClients)

		return TC{
			T:          t,
			nodeClient: nodeClient,
		}
	})

	o.Spec("it returns an error", func(t TC) {
		writer, err := t.nodeClient.Write(context.Background())
		Expect(t, err == nil).To(BeTrue())
		writer.Send(&pb.WriteDataPacket{
			Name:    createName(),
			Message: []byte("some-message"),
		})

		resp, err := writer.CloseAndRecv()
		Expect(t, err == nil).To(BeTrue())
		Expect(t, resp.Error).To(Not(Equal("")))
	})

}

func createName() string {
	return fmt.Sprintf("some-buffer-%d", rand.Int63())
}

func writeTo(name string, data []byte, writer pb.Node_WriteClient) {
	packet := &pb.WriteDataPacket{
		Name:    name,
		Message: data,
	}

	if err := writer.Send(packet); err != nil {
		panic(err)
	}
}

func fetchReaderWithIndex(name string, index uint64, tail bool, client pb.NodeClient) (chan []byte, chan uint64, chan error) {
	c := make(chan []byte, 100)
	idx := make(chan uint64, 100)
	errs := make(chan error, 1)

	bufferInfo := &pb.BufferInfo{
		Name:       name,
		StartIndex: index,
		Tail:       tail,
	}

	reader, err := client.Read(context.Background(), bufferInfo)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			packet, err := reader.Recv()
			if err != nil {
				errs <- err
				return
			}
			c <- packet.Message
			idx <- packet.Index
		}
	}()
	return c, idx, errs
}

func fetchReaderLastIndex(name string, client pb.NodeClient) (chan []byte, chan uint64) {
	c := make(chan []byte, 100)
	idx := make(chan uint64, 100)

	bufferInfo := &pb.BufferInfo{
		Name:         name,
		StartIndex:   1,
		StartFromEnd: true,
		Tail:         true,
	}

	reader, err := client.Read(context.Background(), bufferInfo)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			packet, err := reader.Recv()
			if err != nil {
				return
			}
			c <- packet.Message
			idx <- packet.Index
		}
	}()
	return c, idx
}

func writeSlowly(count int, bufferInfo *pb.BufferInfo, writer pb.Node_WriteClient) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			time.Sleep(time.Millisecond)

			packet := &pb.WriteDataPacket{
				Name:    bufferInfo.Name,
				Message: []byte(fmt.Sprintf("some-data-%d", i)),
			}

			if err := writer.Send(packet); err != nil {
				return
			}
		}
	}()
	return &wg
}

func setupNodeClients(intraPorts, ports []int) map[string]pb.NodeClient {
	clients := make(map[string]pb.NodeClient)
	for _, port := range ports {
		URI := fmt.Sprintf("127.0.0.1:%d", port)
		clients[URI] = connectToNode(port)
	}
	return clients
}

func connectToNode(nodePort int) pb.NodeClient {
	clientConn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", nodePort), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return pb.NewNodeClient(clientConn)
}

func connectToScheduler(schedulerPort int) pb.SchedulerClient {
	clientConn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", schedulerPort), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return pb.NewSchedulerClient(clientConn)
}

func startNode() (int, int, *os.Process) {
	nodePort := end2end.AvailablePort()
	intraNodePort := end2end.AvailablePort()
	path, err := gexec.Build("github.com/apoydence/talaria/node")
	if err != nil {
		panic(err)
	}
	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("ADDR=localhost:%d", nodePort),
		fmt.Sprintf("INTRA_ADDR=localhost:%d", intraNodePort),
	}

	err = command.Start()
	if err != nil {
		panic(err)
	}

	return nodePort, intraNodePort, command.Process
}

func startScheduler(schedulerPort int, nodePorts []int) *os.Process {
	log.Printf("Scheduler Port = %d", schedulerPort)
	for i, port := range nodePorts {
		log.Printf("Node Port (%d) = %d", i, port)
	}

	path, err := gexec.Build("github.com/apoydence/talaria/scheduler")
	if err != nil {
		panic(err)
	}

	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("ADDR=localhost:%d", schedulerPort),
		fmt.Sprintf("NODES=%s", buildNodeURIs(nodePorts)),
	}

	if testing.Verbose() {
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
	}

	err = command.Start()
	if err != nil {
		panic(err)
	}

	return command.Process
}

func buildNodeURIs(ports []int) string {
	var URIs []string
	for _, port := range ports {
		URIs = append(URIs, fmt.Sprintf("127.0.0.1:%d", port))
	}
	return strings.Join(URIs, ",")
}

func fetchNodeClient(URI string, nodeClients map[string]pb.NodeClient) pb.NodeClient {
	client := nodeClients[URI]
	if client == nil {
		log.Panicf("'%s' does not align with a Node server: %v", URI, nodeClients)
	}
	return client
}
