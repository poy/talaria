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

		var fetchReader = func(name string, client pb.TalariaClient) chan []byte {
			c := make(chan []byte, 100)

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
				}
			}()
			return c
		}

		var createFileName = func() string {
			return fmt.Sprintf("some-file-%d", rand.Int63())
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
			talariaClient.Create(context.Background(), fileInfo)
		})

		It("writes data to a subscriber", func() {
			writer, err := talariaClient.Write(context.Background())
			Expect(err).ToNot(HaveOccurred())
			writeTo(fileInfo.FileName, []byte("some-data-1"), writer)
			writeTo(fileInfo.FileName, []byte("some-data-2"), writer)

			data := fetchReader(fileInfo.FileName, talariaClient)
			Eventually(data).Should(Receive(Equal([]byte("some-data-1"))))
			Eventually(data).Should(Receive(Equal([]byte("some-data-2"))))
		})

		It("Read() tails", func() {
			data := fetchReader(fileInfo.FileName, talariaClient)
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
})
