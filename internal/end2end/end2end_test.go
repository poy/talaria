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
			bufferInfo *pb.BufferInfo
			createInfo *pb.CreateInfo
		)

		var writeTo = func(name string, data []byte, writer pb.Talaria_WriteClient) {
			packet := &pb.WriteDataPacket{
				Name:    name,
				Message: data,
			}
			Expect(writer.Send(packet)).To(Succeed())
		}

		var fetchReaderWithIndex = func(name string, index uint64, client pb.TalariaClient) (chan []byte, chan uint64) {
			c := make(chan []byte, 100)
			idx := make(chan uint64, 100)

			bufferInfo = &pb.BufferInfo{
				Name:       name,
				StartIndex: index,
			}

			reader, err := client.Read(context.Background(), bufferInfo)
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

		var fetchReaderLastIndex = func(name string, client pb.TalariaClient) (chan []byte, chan uint64) {
			c := make(chan []byte, 100)
			idx := make(chan uint64, 100)

			bufferInfo = &pb.BufferInfo{
				Name:         name,
				StartIndex:   1,
				StartFromEnd: true,
			}

			reader, err := client.Read(context.Background(), bufferInfo)
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

		var writeSlowly = func(count int, bufferInfo *pb.BufferInfo, writer pb.Talaria_WriteClient) *sync.WaitGroup {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				for i := 0; i < count; i++ {
					time.Sleep(time.Millisecond)
					writeTo(bufferInfo.Name, []byte(fmt.Sprintf("some-data-%d", i)), writer)
				}
			}()
			return &wg
		}

		BeforeEach(func() {
			bufferInfo = &pb.BufferInfo{
				Name: createName(),
			}

			createInfo = &pb.CreateInfo{
				Name: bufferInfo.Name,
			}
		})

		Context("buffer has been created", func() {
			BeforeEach(func() {
				schedulerClient.Create(context.Background(), createInfo)
			})

			Context("start tailing from beginning", func() {
				It("writes data to a subscriber", func() {
					writer, err := nodeClient.Write(context.Background())
					Expect(err).ToNot(HaveOccurred())
					writeTo(bufferInfo.Name, []byte("some-data-1"), writer)
					writeTo(bufferInfo.Name, []byte("some-data-2"), writer)

					data, indexes := fetchReaderWithIndex(bufferInfo.Name, 0, nodeClient)
					Eventually(data).Should(Receive(Equal([]byte("some-data-1"))))
					Eventually(indexes).Should(Receive(BeEquivalentTo(0)))
					Eventually(data).Should(Receive(Equal([]byte("some-data-2"))))
					Eventually(indexes).Should(Receive(BeEquivalentTo(1)))
				})

				It("tails via Read()", func() {
					data, _ := fetchReaderWithIndex(bufferInfo.Name, 0, nodeClient)
					writer, err := nodeClient.Write(context.Background())
					Expect(err).ToNot(HaveOccurred())

					wg := writeSlowly(10, bufferInfo, writer)
					defer wg.Wait()

					for i := 0; i < 10; i++ {
						expectedData := []byte(fmt.Sprintf("some-data-%d", i))
						Eventually(data).Should(Receive(Equal(expectedData)))
					}
				})
			})

			Context("tail from middle", func() {
				It("reads from the given index", func() {
					writer, err := nodeClient.Write(context.Background())
					Expect(err).ToNot(HaveOccurred())
					writeTo(bufferInfo.Name, []byte("some-data-1"), writer)
					writeTo(bufferInfo.Name, []byte("some-data-2"), writer)
					writeTo(bufferInfo.Name, []byte("some-data-3"), writer)

					data, indexes := fetchReaderWithIndex(bufferInfo.Name, 1, nodeClient)

					var idx uint64
					Eventually(indexes).Should(Receive(&idx))
					Expect(idx).To(BeEquivalentTo(1))
					Expect(data).To(Receive(Equal([]byte("some-data-2"))))
				})
			})

			Context("tail from end", func() {
				var waitForData = func() {
					data, _ := fetchReaderWithIndex(bufferInfo.Name, 0, nodeClient)
					Eventually(data).Should(HaveLen(3))
				}

				It("reads from the given index", func() {
					writer, err := nodeClient.Write(context.Background())
					Expect(err).ToNot(HaveOccurred())
					writeTo(bufferInfo.Name, []byte("some-data-1"), writer)
					writeTo(bufferInfo.Name, []byte("some-data-2"), writer)
					writeTo(bufferInfo.Name, []byte("some-data-3"), writer)
					waitForData()

					data, indexes := fetchReaderLastIndex(bufferInfo.Name, nodeClient)

					var idx uint64
					Eventually(indexes).Should(Receive(&idx))
					Expect(idx).To(BeEquivalentTo(2))
					Expect(data).To(Receive(Equal([]byte("some-data-3"))))
				})
			})
		})

		Context("buffer has not been created", func() {
			It("returns an error", func() {
				writer, err := nodeClient.Write(context.Background())
				Expect(err).ToNot(HaveOccurred())

				_, err = writer.CloseAndRecv()
				Expect(err).To(HaveOccurred())
			})
		})
	})
})

func createName() string {
	return fmt.Sprintf("some-buffer-%d", rand.Int63())
}
