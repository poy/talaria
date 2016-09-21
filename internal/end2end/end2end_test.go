package end2end_test

import (
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
				FileName: "some-file",
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

		BeforeEach(func() {
			fileInfo = &pb.File{
				FileName: "some-file",
			}
			talariaClient.Create(context.Background(), fileInfo)
		})

		It("writes data to a subscriber", func() {
			writer, err := talariaClient.Write(context.Background())
			Expect(err).ToNot(HaveOccurred())
			writeTo("some-file", []byte("some-data-1"), writer)
			writeTo("some-file", []byte("some-data-2"), writer)

			data := fetchReader("some-file", talariaClient)
			Eventually(data).Should(Receive(Equal([]byte("some-data-1"))))
			Eventually(data).Should(Receive(Equal([]byte("some-data-2"))))
		})
	})
})
