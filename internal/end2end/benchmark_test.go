package end2end_test

import (
	"math/rand"
	"sync"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Benchmark", func() {

	var randomDataSegment = func() []byte {
		data := make([]byte, 0, rand.Intn(65535))
		for i := 0; i < cap(data); i++ {
			data = append(data, byte(rand.Intn(255)))
		}
		return data
	}

	var randomDataBuilder = func() func() []byte {
		segments := make([][]byte, 0)
		for i := 0; i < 10; i++ {
			segments = append(segments, randomDataSegment())
		}

		return func() []byte {
			result := make([]byte, 0, rand.Intn(3))
			c := cap(result)
			for i := 0; i < c; i++ {
				result = append(result, segments[rand.Intn(len(segments))]...)
			}
			return result
		}
	}

	Context("single buffer", func() {
		var (
			bufferInfo *pb.BufferInfo
		)

		BeforeEach(func() {
			bufferInfo = &pb.BufferInfo{
				Name: createName(),
			}

			_, err := talariaClient.Create(context.Background(), bufferInfo)
			Expect(err).ToNot(HaveOccurred())
		})

		Measure("write 10000 random data points", func(b Benchmarker) {
			writer, err := talariaClient.Write(context.Background())
			Expect(err).ToNot(HaveOccurred())
			randomData := randomDataBuilder()

			b.Time("runtime", func() {
				for i := 0; i < 10000; i++ {
					writer.Send(&pb.WriteDataPacket{
						Name:    bufferInfo.Name,
						Message: randomData(),
					})
				}
			})
		}, 1)

		Measure("reads 10000 random data points", func(b Benchmarker) {
			count := 10000
			writer, err := talariaClient.Write(context.Background())
			Expect(err).ToNot(HaveOccurred())

			reader, err := talariaClient.Read(context.Background(), bufferInfo)
			Expect(err).ToNot(HaveOccurred())
			randomData := randomDataBuilder()

			go func() {
				for i := 0; i < count; i++ {
					writer.Send(&pb.WriteDataPacket{
						Name:    bufferInfo.Name,
						Message: randomData(),
					})
				}
			}()

			b.Time("runtime", func() {
				for i := 0; i < count; i++ {
					reader.Recv()
				}
			})
		}, 1)
	})

	Context("multiple buffers", func() {
		var (
			bufferInfos []*pb.BufferInfo
		)

		BeforeEach(func() {
			for i := 0; i < 5; i++ {
				bufferInfo := &pb.BufferInfo{
					Name: createName(),
				}

				_, err := talariaClient.Create(context.Background(), bufferInfo)
				Expect(err).ToNot(HaveOccurred())
				bufferInfos = append(bufferInfos, bufferInfo)
			}
		})

		Measure("read 100 random data points to each 5 buffers", func(b Benchmarker) {
			count := 100

			b.Time("runtime", func() {

				var wg sync.WaitGroup
				for _, fi := range bufferInfos {
					wg.Add(2)

					go func(info *pb.BufferInfo) {
						defer wg.Done()
						randomData := randomDataBuilder()

						writer, err := talariaClient.Write(context.Background())
						Expect(err).ToNot(HaveOccurred())

						for i := 0; i < count; i++ {
							writer.Send(&pb.WriteDataPacket{
								Name:    info.Name,
								Message: randomData(),
							})
						}

					}(fi)

					go func(info *pb.BufferInfo) {
						defer wg.Done()
						defer GinkgoRecover()

						reader, err := talariaClient.Read(context.Background(), info)
						Expect(err).ToNot(HaveOccurred())

						for i := 0; i < count; i++ {
							reader.Recv()
						}
					}(fi)
				}

				wg.Wait()
			})
		}, 1)
	})
})
