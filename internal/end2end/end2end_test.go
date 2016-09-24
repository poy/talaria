package end2end_test

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("End2end", func() {
	Context("Data has been written", func() {
		var (
			fileInfo *pb.File
		)

		var writeTo = func(name string, data []byte, writer pb.Talaria_WriteClient) {
			packet := &pb.WriteDataPacket{
				FileName: name,
				Message:  data,
			}
			Expect(writer.Send(packet)).To(Succeed())
		}

		var fetchReader = func(name string, client pb.TalariaClient) (chan []byte, chan uint64) {
			c := make(chan []byte, 100)
			idx := make(chan uint64, 100)

			fileInfo = &pb.File{
				FileName: name,
			}

			reader, err := client.Read(context.Background(), fileInfo)
			Expect(err).ToNot(HaveOccurred())

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

		var writeSlowly = func(count int, fileInfo *pb.File, writer pb.Talaria_WriteClient) *sync.WaitGroup {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < count; i++ {
					time.Sleep(time.Millisecond)
					writeTo(fileInfo.FileName, []byte(fmt.Sprintf("some-data-%d", i)), writer)
				}
			}()
			return &wg
		}

		BeforeEach(func() {
			fileInfo = &pb.File{
				FileName: createFileName(),
			}
		})

		Context("file has been created", func() {
			BeforeEach(func() {
				talariaClient.Create(context.Background(), fileInfo)
			})

			It("writes data to a subscriber", func() {
				writer, err := talariaClient.Write(context.Background())
				Expect(err).ToNot(HaveOccurred())
				writeTo(fileInfo.FileName, []byte("some-data-1"), writer)
				writeTo(fileInfo.FileName, []byte("some-data-2"), writer)

				data, indexes := fetchReader(fileInfo.FileName, talariaClient)
				Eventually(data).Should(Receive(Equal([]byte("some-data-1"))))
				Eventually(indexes).Should(Receive(BeEquivalentTo(0)))
				Eventually(data).Should(Receive(Equal([]byte("some-data-2"))))
				Eventually(indexes).Should(Receive(BeEquivalentTo(1)))
			})

			It("Read() tails", func() {
				data, _ := fetchReader(fileInfo.FileName, talariaClient)
				writer, err := talariaClient.Write(context.Background())
				Expect(err).ToNot(HaveOccurred())

				wg := writeSlowly(10, fileInfo, writer)
				defer wg.Wait()

				for i := 0; i < 10; i++ {
					expectedData := []byte(fmt.Sprintf("some-data-%d", i))
					Eventually(data).Should(Receive(Equal(expectedData)))
				}
			})
		})

		Context("file has not been created", func() {
			It("returns an error", func() {
				writer, err := talariaClient.Write(context.Background())
				Expect(err).ToNot(HaveOccurred())

				_, err = writer.CloseAndRecv()
				Expect(err).To(HaveOccurred())
			})
		})
	})
})

func createFileName() string {
	return fmt.Sprintf("some-file-%d", rand.Int63())
}
