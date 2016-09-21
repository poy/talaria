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
	var randomData = func() []byte {
		data := make([]byte, 0, rand.Intn(65535))
		for i := 0; i < cap(data); i++ {
			data = append(data, byte(rand.Int()%255))
		}
		return data
	}

	Context("single file", func() {
		var (
			fileInfo *pb.File
		)

		BeforeEach(func() {
			fileInfo = &pb.File{
				FileName: createFileName(),
			}

			_, err := talariaClient.Create(context.Background(), fileInfo)
			Expect(err).ToNot(HaveOccurred())
		})

		Measure("write 1000 random data points", func(b Benchmarker) {
			writer, err := talariaClient.Write(context.Background())
			Expect(err).ToNot(HaveOccurred())

			b.Time("runtime", func() {
				for i := 0; i < 1000; i++ {
					writer.Send(&pb.WriteDataPacket{
						FileName: fileInfo.FileName,
						Message:  randomData(),
					})
				}
			})
		}, 1)

		Measure("reads 1000 random data points", func(b Benchmarker) {
			count := 1000
			writer, err := talariaClient.Write(context.Background())
			Expect(err).ToNot(HaveOccurred())

			reader, err := talariaClient.Read(context.Background(), fileInfo)
			Expect(err).ToNot(HaveOccurred())

			go func() {
				for i := 0; i < count; i++ {
					writer.Send(&pb.WriteDataPacket{
						FileName: fileInfo.FileName,
						Message:  randomData(),
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

	Context("multiple files", func() {
		var (
			fileInfos []*pb.File
		)

		BeforeEach(func() {
			for i := 0; i < 5; i++ {
				fileInfo := &pb.File{
					FileName: createFileName(),
				}

				_, err := talariaClient.Create(context.Background(), fileInfo)
				Expect(err).ToNot(HaveOccurred())
				fileInfos = append(fileInfos, fileInfo)
			}
		})

		Measure("read 100 random data points to each 5 files", func(b Benchmarker) {
			count := 100

			b.Time("runtime", func() {

				var wg sync.WaitGroup
				for _, fi := range fileInfos {
					wg.Add(2)

					go func(info *pb.File) {
						defer wg.Done()

						writer, err := talariaClient.Write(context.Background())
						Expect(err).ToNot(HaveOccurred())

						for i := 0; i < count; i++ {
							writer.Send(&pb.WriteDataPacket{
								FileName: info.FileName,
								Message:  randomData(),
								// Message: []byte("ADASF"),
							})
						}

					}(fi)

					go func(info *pb.File) {
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
