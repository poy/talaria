//go:generate hel
package server_test

import (
	"fmt"
	"io"
	"net"
	"sync"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/node/internal/server"
	"github.com/apoydence/talaria/pb"
	"google.golang.org/grpc"

	. "github.com/apoydence/eachers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server", func() {
	var (
		mockIOFetcher *mockIOFetcher
		mockWriter    *mockWriter

		listeners []net.Listener
		conns     []*grpc.ClientConn
		s         *server.Server
		client    pb.TalariaClient
	)

	var establishClient = func(URI string) pb.TalariaClient {
		conn, err := grpc.Dial(URI, grpc.WithInsecure())
		Expect(err).ToNot(HaveOccurred())
		conns = append(conns, conn)
		return pb.NewTalariaClient(conn)
	}

	var setupGrpcServer = func(handler pb.TalariaServer) string {
		lis, err := net.Listen("tcp", ":0")
		Expect(err).ToNot(HaveOccurred())
		listeners = append(listeners, lis)
		gs := grpc.NewServer()
		pb.RegisterTalariaServer(gs, handler)
		go gs.Serve(lis)
		return lis.Addr().String()
	}

	BeforeEach(func() {
		listeners = nil
		conns = nil
		mockIOFetcher = newMockIOFetcher()
		mockWriter = newMockWriter()

		s = server.New(mockIOFetcher)
		URI := setupGrpcServer(s)
		client = establishClient(URI)
	})

	JustBeforeEach(func() {
		close(mockIOFetcher.FetchWriterOutput.Ret0)
		close(mockIOFetcher.FetchWriterOutput.Ret1)

		close(mockIOFetcher.FetchReaderOutput.Ret0)
		close(mockIOFetcher.FetchReaderOutput.Ret1)

		close(mockWriter.WriteToOutput.Ret0)
		close(mockWriter.WriteToOutput.Ret1)
	})

	AfterEach(func() {
		for _, lis := range listeners {
			lis.Close()
		}

		for _, conn := range conns {
			conn.Close()
		}
	})

	Describe("Write()", func() {
		var (
			writer pb.Talaria_WriteClient
		)

		var keepWriting = func(p *pb.WriteDataPacket) func() error {
			return func() error {
				return writer.Send(p)
			}
		}

		JustBeforeEach(func() {
			var err error
			writer, err = client.Write(context.Background())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("fetching the buffer does not return an error", func() {
			var (
				info   *pb.BufferInfo
				packet *pb.WriteDataPacket
			)

			BeforeEach(func() {
				info = &pb.BufferInfo{
					Name: "some-name",
				}

				packet = &pb.WriteDataPacket{
					Name:    info.Name,
					Message: []byte("some-data"),
				}

				mockIOFetcher.FetchWriterOutput.Ret0 <- mockWriter
			})

			Context("writer does not return an error", func() {
				It("uses the expected name for the fetcher only once", func() {
					Consistently(keepWriting(packet)).Should(Succeed())

					Expect(mockIOFetcher.FetchWriterInput.Name).To(HaveLen(1))
					Expect(mockIOFetcher.FetchWriterInput.Name).To(BeCalled(With(info.Name)))
				})

				It("writes to the given writer", func() {
					writer.Send(packet)

					Eventually(mockWriter.WriteToInput.Data).Should(BeCalled(With(packet.Message)))
				})
			})

			Context("writer returns an error", func() {
				BeforeEach(func() {
					mockWriter.WriteToOutput.Ret1 <- fmt.Errorf("some-error")
				})

				It("returns an error", func() {
					Eventually(keepWriting(packet)).Should(HaveOccurred())
				})
			})
		})

		Context("fetching the buffer returns an error", func() {
			var (
				packet *pb.WriteDataPacket
			)

			BeforeEach(func() {
				mockIOFetcher.FetchWriterOutput.Ret1 <- fmt.Errorf("some-error")

				packet = &pb.WriteDataPacket{
					Name:    "unknown-name",
					Message: []byte("some-data"),
				}
			})

			It("returns an error", func() {
				Eventually(keepWriting(packet)).Should(HaveOccurred())
			})
		})
	})

	Describe("Read()", func() {
		var (
			mReader        *mockReader
			reader         pb.Talaria_ReadClient
			info           *pb.BufferInfo
			startReadingWg *sync.WaitGroup
			ctx            context.Context
			cancel         context.CancelFunc
		)

		var setupReader = func() {
			ctx, cancel = context.WithCancel(context.Background())
			var err error
			reader, err = client.Read(ctx, info)
			Expect(err).ToNot(HaveOccurred())
		}

		var startReading = func(reader pb.Talaria_ReadClient) (chan []byte, chan uint64, chan error) {
			startReadingWg.Add(1)
			d := make(chan []byte, 100)
			i := make(chan uint64, 100)
			e := make(chan error, 100)
			go func(r pb.Talaria_ReadClient) {
				defer startReadingWg.Done()
				for {
					packet, err := r.Recv()
					if err != nil {
						e <- err
						return
					}

					d <- packet.Message
					i <- packet.Index
				}
			}(reader)

			return d, i, e
		}

		var writeToReader = func(reader *mockReader, data []byte, idx uint64, err error) {
			reader.ReadAtOutput.Ret0 <- data
			reader.ReadAtOutput.Ret1 <- idx
			reader.ReadAtOutput.Ret2 <- err
		}

		BeforeEach(func() {
			startReadingWg = new(sync.WaitGroup)
			info = &pb.BufferInfo{
				Name: "some-name",
			}

			mReader = newMockReader()
			mockIOFetcher.FetchReaderOutput.Ret0 <- mReader
		})

		JustBeforeEach(func() {
			close(mReader.LastIndexOutput.Ret0)
		})

		AfterEach(func(done Done) {
			defer close(done)
			cancel()

			startReadingWg.Wait()
		})

		Context("fetching the buffer does not return an error", func() {
			Context("reader doesn't return an error", func() {
				Context("start index is 0 (beginning)", func() {
					BeforeEach(func() {
						setupReader()
					})

					It("uses the expected name for the fetcher only once", func() {
						startReading(reader)

						Eventually(mockIOFetcher.FetchReaderInput.Name).Should(HaveLen(1))
						Consistently(mockIOFetcher.FetchReaderInput.Name).Should(HaveLen(1))
						Expect(mockIOFetcher.FetchReaderInput.Name).To(BeCalled(With(info.Name)))
					})

					It("returns data from the reader", func() {
						data, _, _ := startReading(reader)
						writeToReader(mReader, []byte("some-data"), 0, nil)

						Eventually(data).Should(Receive(Equal([]byte("some-data"))))
					})

					It("returns indexes from the reader", func() {
						_, indexes, _ := startReading(reader)
						writeToReader(mReader, []byte("some-data"), 0, nil)
						writeToReader(mReader, []byte("some-data"), 1, nil)
						writeToReader(mReader, []byte("some-data"), 2, nil)

						Eventually(indexes).Should(Receive(BeEquivalentTo(0)))
						Eventually(indexes).Should(Receive(BeEquivalentTo(1)))
						Eventually(indexes).Should(Receive(BeEquivalentTo(2)))
					})

					It("increments the index each read", func() {
						writeToReader(mReader, []byte("some-data-0"), 0, nil)
						writeToReader(mReader, []byte("some-data-1"), 1, nil)

						Eventually(mReader.ReadAtInput.Index).Should(Receive(BeEquivalentTo(0)))
						Eventually(mReader.ReadAtInput.Index).Should(Receive(BeEquivalentTo(1)))
					})

					Describe("tails the reader", func() {
						It("waits and then tries again", func() {
							_, _, errs := startReading(reader)
							writeToReader(mReader, nil, 0, io.EOF)
							writeToReader(mReader, nil, 0, io.EOF)

							Eventually(mReader.ReadAtCalled).Should(HaveLen(3))
							Expect(errs).To(BeEmpty())
						})
					})
				})

				Context("start index is set to non-zero", func() {
					BeforeEach(func() {
						info.StartIndex = 1
						setupReader()
					})

					It("starts at the given index", func() {
						writeToReader(mReader, []byte("some-data-3"), 0, nil)
						writeToReader(mReader, []byte("some-data-1"), 1, nil)
						writeToReader(mReader, []byte("some-data-2"), 2, nil)

						var idx uint64
						Eventually(mReader.ReadAtInput.Index).Should(Receive(&idx))
						Expect(idx).To(BeEquivalentTo(1))
					})
				})

				Context("StartFronEnd is set to true", func() {
					BeforeEach(func() {
						info.StartIndex = 1
						info.StartFromEnd = true
						mReader.LastIndexOutput.Ret0 <- 2

						setupReader()
					})

					It("starts from the end", func() {
						writeToReader(mReader, []byte("some-data-3"), 0, nil)
						writeToReader(mReader, []byte("some-data-1"), 1, nil)
						writeToReader(mReader, []byte("some-data-2"), 2, nil)

						var idx uint64
						Eventually(mReader.ReadAtInput.Index).Should(Receive(&idx))
						Expect(idx).To(BeEquivalentTo(2))
					})
				})
			})

			Context("reader returns an error", func() {
				BeforeEach(func() {
					setupReader()
				})

				It("returns an error", func() {
					_, _, errs := startReading(reader)
					writeToReader(mReader, nil, 0, fmt.Errorf("some-error"))

					Eventually(errs).ShouldNot(BeEmpty())
				})
			})
		})

		Context("fetching the buffer returns an error", func() {
			BeforeEach(func() {
				mockIOFetcher.FetchReaderOutput.Ret1 <- fmt.Errorf("some-error")

				setupReader()
			})

			It("returns an error", func() {
				_, _, errs := startReading(reader)

				Eventually(errs).ShouldNot(BeEmpty())
			})
		})
	})
})
