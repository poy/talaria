package end2end_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/internal/end2end"
	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/onsi/gomega/gexec"
)

type TC struct {
	*testing.T
	bufferInfo      *pb.BufferInfo
	createInfo      *intra.CreateInfo
	nodePort        int
	nodeProcess     *os.Process
	closers         []io.Closer
	nodeClient      pb.TalariaClient
	intraNodeClient intra.NodeClient
}

func TestNodeEnd2EndBufferCreated(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TC {
		nodePort, session := startNode(t)

		nodeClient, closer := connectToNode(t, nodePort)
		intraNodeClient, closerIntra := connectToIntraNode(t, nodePort)

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

	o.Group("when tailing from beginning", func() {
		o.Spec("it writes data to a subscriber", func(t TC) {
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())
			writeTo(t.bufferInfo.Name, []byte("some-data-1"), writer)
			writeTo(t.bufferInfo.Name, []byte("some-data-2"), writer)

			data, indexes := fetchReaderWithIndex(t.bufferInfo.Name, 0, t.nodeClient)
			Expect(t, data).To(ViaPolling(
				Chain(Receive(), Equal([]byte("some-data-1"))),
			))
			Expect(t, indexes).To(ViaPolling(
				Chain(Receive(), Equal(uint64(0))),
			))
			Expect(t, data).To(ViaPolling(
				Chain(Receive(), Equal([]byte("some-data-2"))),
			))
			Expect(t, indexes).To(ViaPolling(
				Chain(Receive(), Equal(uint64(1))),
			))
		})

		o.Spec("it tails via Read()", func(t TC) {
			data, _ := fetchReaderWithIndex(t.bufferInfo.Name, 0, t.nodeClient)
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())

			wg := writeSlowly(10, t.bufferInfo, writer)
			defer wg.Wait()

			for i := 0; i < 10; i++ {
				expectedData := []byte(fmt.Sprintf("some-data-%d", i))
				Expect(t, data).To(ViaPolling(
					Chain(Receive(), Equal(expectedData)),
				))
			}
		})
	})

	o.Group("when tailing from middle", func() {
		o.Spec("it reads from the given index", func(t TC) {
			writer, err := t.nodeClient.Write(context.Background())
			Expect(t, err == nil).To(BeTrue())
			writeTo(t.bufferInfo.Name, []byte("some-data-1"), writer)
			writeTo(t.bufferInfo.Name, []byte("some-data-2"), writer)
			writeTo(t.bufferInfo.Name, []byte("some-data-3"), writer)

			data, indexes := fetchReaderWithIndex(t.bufferInfo.Name, 1, t.nodeClient)

			Expect(t, indexes).To(ViaPolling(
				Chain(Receive(), Equal(uint64(1))),
			))
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

			data, indexes := fetchReaderLastIndex(t.bufferInfo.Name, t.nodeClient)

			Expect(t, indexes).To(ViaPolling(
				Chain(Receive(), Equal(uint64(2))),
			))
			Expect(t, data).To(ViaPolling(
				Chain(Receive(), Equal([]byte("some-data-3"))),
			))
		})
	})
}

func TestNodeEnd2EndBufferNotCreated(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TC {
		nodePort, session := startNode(t)

		nodeClient, closer := connectToNode(t, nodePort)
		intraNodeClient, closerIntra := connectToIntraNode(t, nodePort)

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
		var client pb.Talaria_ReadClient

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

func writeSlowly(count int, bufferInfo *pb.BufferInfo, writer pb.Talaria_WriteClient) *sync.WaitGroup {
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

func connectToNode(t *testing.T, nodePort int) (pb.TalariaClient, io.Closer) {
	clientConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", nodePort), grpc.WithInsecure())
	Expect(t, err == nil).To(BeTrue())

	return pb.NewTalariaClient(clientConn), clientConn
}

func connectToIntraNode(t *testing.T, nodePort int) (intra.NodeClient, io.Closer) {
	clientConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", nodePort), grpc.WithInsecure())
	Expect(t, err == nil).To(BeTrue())

	return intra.NewNodeClient(clientConn), clientConn
}

func startNode(t *testing.T) (int, *os.Process) {
	nodePort := end2end.AvailablePort()

	path, err := gexec.Build("github.com/apoydence/talaria/node")
	Expect(t, err == nil).To(BeTrue())
	command := exec.Command(path)
	command.Env = []string{
		fmt.Sprintf("PORT=%d", nodePort),
	}

	err = command.Start()
	Expect(t, err == nil).To(BeTrue())

	return nodePort, command.Process
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
