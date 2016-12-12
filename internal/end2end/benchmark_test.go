package end2end_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/pb"
)

func BenchmarkSingleBufferWrite(b *testing.B) {
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
	Expect(b, f).To(ViaPollingMatcher{
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

	Expect(b, f).To(ViaPollingMatcher{
		Matcher:  BeTrue(),
		Duration: 5 * time.Second,
	})

	b.ResetTimer()

	writer, err := nodeClient.Write(context.Background())
	Expect(b, err == nil).To(BeTrue())
	randomData := randomDataBuilder()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.Send(&pb.WriteDataPacket{
			Name:    bufferInfo.Name,
			Message: randomData(),
		})
		Expect(b, err == nil).To(BeTrue())
	}
}

func BenchmarkSingleBufferRead(b *testing.B) {
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
	Expect(b, f).To(ViaPollingMatcher{
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

	Expect(b, f).To(ViaPollingMatcher{
		Matcher:  BeTrue(),
		Duration: 5 * time.Second,
	})

	writer, err := nodeClient.Write(context.Background())
	Expect(b, err == nil).To(BeTrue())

	reader, err := nodeClient.Read(context.Background(), bufferInfo)
	Expect(b, err == nil).To(BeTrue())
	randomData := randomDataBuilder()

	go func(n int) {
		for i := 0; i < n; i++ {
			err := writer.Send(&pb.WriteDataPacket{
				Name:    bufferInfo.Name,
				Message: randomData(),
			})

			if err != nil {
				panic(err)
			}
		}
	}(b.N)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := reader.Recv()
		Expect(b, err == nil).To(BeTrue())
	}
}

func BenchmarkCreatingBuffers(b *testing.B) {
	schedulerClient := connectToScheduler(schedulerPort)

	for i := 0; i < b.N; i++ {
		bufferInfo := &pb.BufferInfo{
			Name: createName(),
		}

		createInfo := &pb.CreateInfo{
			Name: bufferInfo.Name,
		}

		f := func() bool {
			_, err := schedulerClient.Create(context.Background(), createInfo)
			if err != nil {
				return false
			}
			return true
		}
		Expect(b, f).To(ViaPollingMatcher{
			Matcher:  BeTrue(),
			Duration: 1 * time.Second,
		})
	}
}

func BenchmarkMultipleBuffersRead(b *testing.B) {
	nodeClients := setupNodeClients(nodePorts)
	schedulerClient := connectToScheduler(schedulerPort)

	var (
		bufferInfos []*pb.BufferInfo
		createInfos []*pb.CreateInfo
		clients     []pb.TalariaClient
	)

	for i := 0; i < 5; i++ {
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
		Expect(b, f).To(ViaPollingMatcher{
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

		Expect(b, f).To(ViaPollingMatcher{
			Matcher:  BeTrue(),
			Duration: 5 * time.Second,
		})

		bufferInfos = append(bufferInfos, bufferInfo)
		createInfos = append(createInfos, createInfo)
		clients = append(clients, nodeClient)
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	for i, fi := range bufferInfos {
		wg.Add(2)

		go func(client pb.TalariaClient, info *pb.BufferInfo, n int) {
			defer wg.Done()
			randomData := randomDataBuilder()

			writer, err := client.Write(context.Background())
			Expect(b, err == nil).To(BeTrue())

			for i := 0; i < n; i++ {
				data := randomData()
				err := writer.Send(&pb.WriteDataPacket{
					Name:    info.Name,
					Message: data,
				})
				if err != nil {
					panic(err)
				}
			}

		}(clients[i], fi, b.N)

		go func(client pb.TalariaClient, info *pb.BufferInfo, n int) {
			defer wg.Done()

			reader, err := client.Read(context.Background(), info)
			Expect(b, err == nil).To(BeTrue())

			for i := 0; i < n; i++ {
				reader.Recv()
			}
		}(clients[i], fi, b.N)
	}

	wg.Wait()
}

func randomDataSegment() []byte {
	data := make([]byte, 0, rand.Intn(65535))
	for i := 0; i < cap(data); i++ {
		data = append(data, byte(rand.Intn(255)))
	}
	return data
}

func randomDataBuilder() func() []byte {
	segments := make([][]byte, 0)
	for i := 0; i < 10; i++ {
		segments = append(segments, randomDataSegment())
	}

	return func() []byte {
		result := make([]byte, 0, rand.Intn(2)+1)
		c := cap(result)
		for i := 0; i < c; i++ {
			result = append(result, segments[rand.Intn(len(segments))]...)
		}

		return result
	}
}
