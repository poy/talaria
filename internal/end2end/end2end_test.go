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
	"github.com/apoydence/talaria/internal/end2end"
	"github.com/apoydence/talaria/pb"
	"github.com/onsi/gomega/gexec"
)

var (
	nodePorts     []int
	schedulerPort int
)

func setup() []*os.Process {
	schedulerPort = end2end.AvailablePort()
	nodePort1, nodeProcess1 := startNode(schedulerPort)
	nodePort2, nodeProcess2 := startNode(schedulerPort)
	nodePort3, nodeProcess3 := startNode(schedulerPort)
	nodePorts = []int{nodePort1, nodePort2, nodePort3}
	var schedulerProcess *os.Process
	schedulerProcess = startScheduler(schedulerPort, nodePorts)

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
	bufferInfo *pb.BufferInfo
	createInfo *pb.CreateInfo
	nodeClient pb.TalariaClient
}

func TestEnd2EndBufferHasBeenCreated(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TC {
		nodeClients := setupNodeClients(nodePorts)
		schedulerClient := connectToScheduler(schedulerPort)

		bufferInfo := &pb.BufferInfo{
			Name: createName(),
		}

		createInfo := &pb.CreateInfo{
			Name: bufferInfo.Name,
		}

		var nodeClient pb.TalariaClient
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
			T:          t,
			bufferInfo: bufferInfo,
			createInfo: createInfo,
			nodeClient: nodeClient,
		}
	})

	o.Group("when tailing from beginning", func() {
		o.Spec("it writes data to a subscriber", func(t TC) {
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())
			writeTo(t.bufferInfo.Name, []byte("some-data-1"), writer)
			writeTo(t.bufferInfo.Name, []byte("some-data-2"), writer)

			data, _ := fetchReaderWithIndex(t.bufferInfo.Name, 0, t.nodeClient)
			Expect(t, data).To(ViaPolling(
				Chain(Receive(), Equal([]byte("some-data-1"))),
			))
			Expect(t, data).To(ViaPolling(
				Chain(Receive(), Equal([]byte("some-data-2"))),
			))
		})

		o.Spec("it tails via Read", func(t TC) {
			data, _ := fetchReaderWithIndex(t.bufferInfo.Name, 0, t.nodeClient)
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())

			wg := writeSlowly(10, t.bufferInfo, writer)
			defer wg.Wait()

			for i := 0; i < 10; i++ {
				expectedData := []byte(fmt.Sprintf("some-data-%d", i))
				Expect(t, data).To(ViaPollingMatcher{
					Matcher:  Chain(Receive(), Equal(expectedData)),
					Duration: 5 * time.Second,
				})
			}
		})

		o.Group("when tailing from middle", func() {
			o.Spec("it reads from the given index", func(t TC) {
				writer, err := t.nodeClient.Write(context.Background())
				Expect(t, err == nil).To(BeTrue())
				writeTo(t.bufferInfo.Name, []byte("some-data-1"), writer)
				writeTo(t.bufferInfo.Name, []byte("some-data-2"), writer)
				writeTo(t.bufferInfo.Name, []byte("some-data-3"), writer)

				data, _ := fetchReaderWithIndex(t.bufferInfo.Name, 1, t.nodeClient)

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
				data, _ := fetchReaderWithIndex(t.bufferInfo.Name, 0, t.nodeClient)
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
		nodeClients := setupNodeClients(nodePorts)
		schedulerClient := connectToScheduler(schedulerPort)

		bufferInfo := &pb.BufferInfo{
			Name: createName(),
		}

		createInfo := &pb.CreateInfo{
			Name: bufferInfo.Name,
		}

		var nodeClient pb.TalariaClient
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
			T:          t,
			bufferInfo: bufferInfo,
			createInfo: createInfo,
			nodeClient: nodeClient,
		}
	})

	o.Spec("it returns an error", func(t TC) {
		writer, err := t.nodeClient.Write(context.Background())
		Expect(t, err == nil).To(BeTrue())

		_, err = writer.CloseAndRecv()
		Expect(t, err == nil).To(BeFalse())
	})

}

func createName() string {
	return fmt.Sprintf("some-buffer-%d", rand.Int63())
}

func writeTo(name string, data []byte, writer pb.Talaria_WriteClient) {
	packet := &pb.WriteDataPacket{
		Name:    name,
		Message: data,
	}

	if err := writer.Send(packet); err != nil {
		panic(err)
	}
}

func fetchReaderWithIndex(name string, index uint64, client pb.TalariaClient) (chan []byte, chan uint64) {
	c := make(chan []byte, 100)
	idx := make(chan uint64, 100)

	bufferInfo := &pb.BufferInfo{
		Name:       name,
		StartIndex: index,
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

func fetchReaderLastIndex(name string, client pb.TalariaClient) (chan []byte, chan uint64) {
	c := make(chan []byte, 100)
	idx := make(chan uint64, 100)

	bufferInfo := &pb.BufferInfo{
		Name:         name,
		StartIndex:   1,
		StartFromEnd: true,
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

func writeSlowly(count int, bufferInfo *pb.BufferInfo, writer pb.Talaria_WriteClient) *sync.WaitGroup {
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

func setupNodeClients(ports []int) map[string]pb.TalariaClient {
	clients := make(map[string]pb.TalariaClient)
	for _, port := range ports {
		URI := fmt.Sprintf("localhost:%d", port)
		clients[URI] = connectToNode(port)
	}
	return clients
}

func connectToNode(nodePort int) pb.TalariaClient {
	clientConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", nodePort), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return pb.NewTalariaClient(clientConn)
}

func connectToScheduler(schedulerPort int) pb.SchedulerClient {
	clientConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", schedulerPort), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return pb.NewSchedulerClient(clientConn)
}

func startNode(schedulerPort int) (int, *os.Process) {
	nodePort := end2end.AvailablePort()
	path, err := gexec.Build("github.com/apoydence/talaria/node")
	if err != nil {
		panic(err)
	}
	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("PORT=%d", nodePort),
		fmt.Sprintf("SCHEDULER_URI=localhost:%d", schedulerPort),
	}

	err = command.Start()
	if err != nil {
		panic(err)
	}

	return nodePort, command.Process
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
		fmt.Sprintf("PORT=%d", schedulerPort),
		fmt.Sprintf("NODES=%s", buildNodeURIs(nodePorts)),
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
		URIs = append(URIs, fmt.Sprintf("localhost:%d", port))
	}
	return strings.Join(URIs, ",")
}

func fetchNodeClient(URI string, nodeClients map[string]pb.TalariaClient) pb.TalariaClient {
	client := nodeClients[URI]
	if client == nil {
		log.Panic(fmt.Sprintf("'%s' does not align with a Node server", URI))
	}
	return client
}
