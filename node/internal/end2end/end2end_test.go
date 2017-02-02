package end2end_test

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/api/intra"
	pb "github.com/apoydence/talaria/api/v1"
	"github.com/apoydence/talaria/internal/end2end"
	"github.com/onsi/gomega/gexec"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		grpclog.SetLogger(log.New(ioutil.Discard, "", 0))
	}

	os.Exit(m.Run())
}

type TC struct {
	*testing.T
	bufferInfo      *pb.BufferInfo
	createInfo      *intra.CreateInfo
	nodePort        int
	nodeProcess     *os.Process
	closers         []io.Closer
	nodeClient      pb.NodeClient
	intraNodeClient intra.NodeClient
}

func TestNodeEnd2EndBufferCreated(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TC {
		nodePort, intraNodePort, session := startNode(t)

		nodeClient, closer := connectToNode(t, nodePort)
		intraNodeClient, closerIntra := connectToIntraNode(t, intraNodePort)

		bufferInfo := &pb.BufferInfo{
			Name: createName(),
		}

		createInfo := &intra.CreateInfo{
			Name: bufferInfo.Name,
		}

		f := func() bool {
			_, err := intraNodeClient.Create(context.Background(), createInfo)
			return err == nil
		}
		Expect(t, f).To(ViaPollingMatcher{
			Matcher:  BeTrue(),
			Duration: 5 * time.Second,
		})

		leaderF := func() string {
			resp, err := intraNodeClient.Leader(context.Background(), &intra.LeaderRequest{
				Name: bufferInfo.Name,
			})

			if err != nil {
				return ""
			}

			return resp.Addr
		}

		Expect(t, leaderF).To(ViaPollingMatcher{
			Matcher:  Equal(fmt.Sprintf("127.0.0.1:%d", intraNodePort)),
			Duration: 5 * time.Second,
		})

		return TC{
			T:               t,
			bufferInfo:      bufferInfo,
			createInfo:      createInfo,
			nodePort:        nodePort,
			nodeProcess:     session,
			closers:         []io.Closer{closer, closerIntra},
			nodeClient:      nodeClient,
			intraNodeClient: intraNodeClient,
		}
	})

	o.AfterEach(func(t TC) {
		for _, closer := range t.closers {
			closer.Close()
		}

		t.nodeProcess.Kill()
		t.nodeProcess.Wait()
	})

	o.Spec("it lists the buffer", func(t TC) {
		ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
		resp, err := t.nodeClient.ListClusters(ctx, new(pb.ListClustersInfo))
		Expect(t, err == nil).To(BeTrue())
		Expect(t, resp.Names).To(HaveLen(1))
		Expect(t, resp.Names).To(Contain(t.bufferInfo.Name))
	})

	o.Group("when tailing from beginning", func() {
		o.Spec("it writes data to a subscriber", func(t TC) {
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())
			writeTo(t.bufferInfo.Name, []byte("some-data-1"), writer)
			writeTo(t.bufferInfo.Name, []byte("some-data-2"), writer)

			data, _, _ := fetchReaderWithIndex(t.bufferInfo.Name, 0, true, t.nodeClient)
			Expect(t, data).To(ViaPollingMatcher{
				Duration: 3 * time.Second,
				Matcher:  Chain(Receive(), Equal([]byte("some-data-1"))),
			})

			Expect(t, data).To(ViaPolling(
				Chain(Receive(), Equal([]byte("some-data-2"))),
			))
		})

		o.Spec("it fails to write to a read only buffer", func(t TC) {
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())

			_, err = t.intraNodeClient.ReadOnly(context.Background(), &intra.ReadOnlyInfo{
				Name: t.bufferInfo.Name,
			})
			Expect(t, err == nil).To(BeTrue())

			packet := &pb.WriteDataPacket{
				Name:    t.bufferInfo.Name,
				Message: []byte("some-data-1"),
			}

			f := func() bool {
				return writer.Send(packet) == nil
			}
			Expect(t, f).To(ViaPolling(BeFalse()))
		})

		o.Spec("it tails via Read", func(t TC) {
			data, _, _ := fetchReaderWithIndex(t.bufferInfo.Name, 0, true, t.nodeClient)
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())

			wg := writeSlowly(10, t.bufferInfo, writer)
			defer wg.Wait()

			for i := 0; i < 10; i++ {
				expectedData := []byte(fmt.Sprintf("some-data-%d", i))
				Expect(t, data).To(ViaPollingMatcher{
					Duration: 3 * time.Second,
					Matcher:  Chain(Receive(), Equal(expectedData)),
				})
			}
		})

		o.Spec("it reads only whats available via Read", func(t TC) {
			_, _, errs := fetchReaderWithIndex(t.bufferInfo.Name, 0, false, t.nodeClient)

			Expect(t, errs).To(ViaPollingMatcher{
				Duration: 3 * time.Second,
				Matcher:  Not(HaveLen(0)),
			})
		})
	})

	o.Group("when tailing from middle", func() {
		o.Spec("it reads from the given index", func(t TC) {
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())
			writeTo(t.bufferInfo.Name, []byte("some-data-1"), writer)
			writeTo(t.bufferInfo.Name, []byte("some-data-2"), writer)
			writeTo(t.bufferInfo.Name, []byte("some-data-3"), writer)

			data, _, _ := fetchReaderWithIndex(t.bufferInfo.Name, 0, true, t.nodeClient)
			Expect(t, data).To(ViaPollingMatcher{
				Matcher:  Chain(Receive(), Equal([]byte("some-data-2"))),
				Duration: 5 * time.Second,
			})
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
			Expect(t, data).To(ViaPollingMatcher{
				Duration: 3 * time.Second,
				Matcher:  HaveLen(3),
			})

			data, _ = fetchReaderLastIndex(t.bufferInfo.Name, t.nodeClient)

			Expect(t, data).To(ViaPolling(
				Chain(Receive(), Equal([]byte("some-data-3"))),
			))
		})
	})

	o.Group("when reporting status", func() {
		o.Spec("it gives the list of buffers", func(t TC) {
			status, err := t.intraNodeClient.Status(context.Background(), new(intra.StatusRequest))
			Expect(t, err == nil).To(BeTrue())
			Expect(t, status.Buffers).To(HaveLen(1))
			Expect(t, status.Buffers[0].Name).To(Equal(t.bufferInfo.Name))
		})
	})
}

func TestNodeEnd2EndBufferNotCreated(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TC {
		nodePort, intraNodePort, session := startNode(t)

		nodeClient, closer := connectToNode(t, nodePort)
		intraNodeClient, closerIntra := connectToIntraNode(t, intraNodePort)

		bufferInfo := &pb.BufferInfo{
			Name: createName(),
		}

		createInfo := &intra.CreateInfo{
			Name: bufferInfo.Name,
		}

		return TC{
			T:               t,
			bufferInfo:      bufferInfo,
			createInfo:      createInfo,
			nodePort:        nodePort,
			nodeProcess:     session,
			closers:         []io.Closer{closer, closerIntra},
			nodeClient:      nodeClient,
			intraNodeClient: intraNodeClient,
		}
	})

	o.AfterEach(func(t TC) {
		for _, closer := range t.closers {
			closer.Close()
		}

		t.nodeProcess.Kill()
		t.nodeProcess.Wait()
	})

	o.Spec("it returns an error", func(t TC) {
		var client pb.Node_ReadClient

		f := func() bool {
			var err error
			client, err = t.nodeClient.Read(context.Background(), t.bufferInfo)
			return err == nil
		}
		Expect(t, f).To(ViaPolling(BeTrue()))

		_, err := client.Recv()
		Expect(t, err == nil).To(BeFalse())
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

func writeSlowly(count int, bufferInfo *pb.BufferInfo, writer pb.Node_WriteClient) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			time.Sleep(time.Millisecond)
			writeTo(bufferInfo.Name, []byte(fmt.Sprintf("some-data-%d", i)), writer)
		}
	}()
	return &wg
}

func connectToNode(t *testing.T, nodePort int) (pb.NodeClient, io.Closer) {
	clientConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", nodePort), grpc.WithInsecure())
	Expect(t, err == nil).To(BeTrue())

	return pb.NewNodeClient(clientConn), clientConn
}

func connectToIntraNode(t *testing.T, nodePort int) (intra.NodeClient, io.Closer) {
	clientConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", nodePort), grpc.WithInsecure())
	Expect(t, err == nil).To(BeTrue())

	return intra.NewNodeClient(clientConn), clientConn
}

func startNode(t *testing.T) (int, int, *os.Process) {
	nodePort := end2end.AvailablePort()
	intraNodePort := end2end.AvailablePort()

	path, err := gexec.Build("github.com/apoydence/talaria/node")
	Expect(t, err == nil).To(BeTrue())
	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("ADDR=localhost:%d", nodePort),
		fmt.Sprintf("INTRA_ADDR=localhost:%d", intraNodePort),
	}

	if testing.Verbose() {
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
	}

	err = command.Start()
	Expect(t, err == nil).To(BeTrue())

	return nodePort, intraNodePort, command.Process
}

func fetchReaderLastIndex(name string, client pb.NodeClient) (chan []byte, chan uint64) {
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
