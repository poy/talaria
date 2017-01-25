//go:generate hel
package server_test

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"testing"

	"golang.org/x/net/context"

	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/onpar"
	"github.com/apoydence/talaria/node/internal/server"
	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/stored"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		grpclog.SetLogger(log.New(ioutil.Discard, "", 0))
	}

	os.Exit(m.Run())
}

type TT struct {
	*testing.T
	mockIOFetcher  *mockIOFetcher
	mockWriter     *mockWriter
	handlerWrapper *handlerWrapper
	s              *server.Server
	client         pb.NodeClient
	closers        []io.Closer
}

type TW struct {
	TT
	writer pb.Node_WriteClient
	info   *pb.BufferInfo
	packet *pb.WriteDataPacket
}

func TestServerWrite(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TW {
		mockIOFetcher := newMockIOFetcher()
		mockWriter := newMockWriter()

		s := server.New(mockIOFetcher)
		lis, handlerWrapper := setupGrpcServer(s)
		client, conn := establishClient(lis.Addr().String())

		writer, err := client.Write(context.Background())
		Expect(t, err == nil).To(Equal(true))

		tt := TT{
			mockIOFetcher:  mockIOFetcher,
			mockWriter:     mockWriter,
			handlerWrapper: handlerWrapper,
			s:              s,
			client:         client,
			closers:        []io.Closer{lis, conn},
		}

		tt.T = t

		return TW{
			TT:     tt,
			writer: writer,
		}
	})

	o.AfterEach(func(t TW) {
		for _, c := range t.closers {
			c.Close()
		}
	})

	o.Group("when fetching the buffer does not return an error", func() {
		o.BeforeEach(func(t TW) TW {
			t.info = &pb.BufferInfo{
				Name: "some-name",
			}

			t.packet = &pb.WriteDataPacket{
				Name:    t.info.Name,
				Message: []byte("some-data"),
			}

			t.mockIOFetcher.FetchWriterOutput.Ret0 <- t.mockWriter
			t.mockIOFetcher.FetchWriterOutput.Ret1 <- nil

			return t
		})

		o.Group("when writer does not return an error", func() {
			o.BeforeEach(func(t TW) TW {
				close(t.mockWriter.WriteOutput.Ret0)
				return t
			})

			o.Spec("it uses the expected name for the fetcher only once", func(t TW) {
				Expect(t, keepWriting(t.writer, t.packet)).To(Always(Equal(true)))
				Expect(t, t.mockIOFetcher.FetchWriterInput.Name).To(HaveLen(1))
				Expect(t, t.mockIOFetcher.FetchWriterInput.Name).To(
					Chain(Receive(), Equal(t.info.Name)),
				)
			})

			o.Spec("it writes to the given writer", func(t TW) {
				err := t.writer.Send(t.packet)
				Expect(t, err == nil).To(BeTrue())

				expectedMsg := stored.Data{
					Payload: t.packet.Message,
					Type:    stored.Data_Normal,
				}

				Expect(t, t.mockWriter.WriteInput.Data).To(ViaPolling(Chain(
					Receive(), Equal(expectedMsg),
				)))
			})

			o.Spec("it returns the number of successful writes", func(t TW) {
				err := t.writer.Send(t.packet)
				Expect(t, err == nil).To(BeTrue())

				resp, err := t.writer.CloseAndRecv()
				Expect(t, err == nil).To(BeTrue())
				Expect(t, resp.LastWriteIndex).To(Equal(uint64(1)))
			})
		})

		o.Group("when writer returns an error", func() {
			o.BeforeEach(func(t TW) TW {
				t.mockWriter.WriteOutput.Ret0 <- fmt.Errorf("some-error")
				t.mockIOFetcher.FetchWriterOutput.Ret0 <- t.mockWriter
				t.mockIOFetcher.FetchWriterOutput.Ret1 <- nil

				return t
			})

			o.Spec("returns an error", func(t TW) {
				Expect(t, keepWriting(t.writer, t.packet)).To(ViaPolling(
					Equal(false)),
				)
			})
		})
	})

	o.Group("when fetching the buffer returns an error", func() {
		o.BeforeEach(func(t TW) TW {
			t.mockIOFetcher.FetchWriterOutput.Ret0 <- nil
			t.mockIOFetcher.FetchWriterOutput.Ret1 <- fmt.Errorf("some-error")

			t.packet = &pb.WriteDataPacket{
				Name:    "unknown-name",
				Message: []byte("some-data"),
			}

			return t
		})

		o.Spec("returns an error", func(t TW) {
			Expect(t, keepWriting(t.writer, t.packet)).To(ViaPolling(
				Equal(false)),
			)
		})
	})

}

type TR struct {
	TT
	mReader        *mockReader
	reader         pb.Node_ReadClient
	info           *pb.BufferInfo
	startReadingWg *sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

func TestServerRead(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TR {
		mockIOFetcher := newMockIOFetcher()
		mockWriter := newMockWriter()

		s := server.New(mockIOFetcher)
		lis, handlerWrapper := setupGrpcServer(s)
		client, conn := establishClient(lis.Addr().String())

		startReadingWg := new(sync.WaitGroup)
		info := &pb.BufferInfo{
			Name: "some-name",
		}

		mReader := newMockReader()
		mockIOFetcher.FetchReaderOutput.Ret0 <- mReader

		ctx, cancel := context.WithCancel(context.Background())

		tt := TT{
			mockIOFetcher:  mockIOFetcher,
			mockWriter:     mockWriter,
			handlerWrapper: handlerWrapper,
			s:              s,
			client:         client,
			closers:        []io.Closer{lis, conn},
		}

		tt.T = t

		return TR{
			TT:             tt,
			mReader:        mReader,
			info:           info,
			startReadingWg: startReadingWg,
			ctx:            ctx,
			cancel:         cancel,
		}
	})

	o.AfterEach(func(t TR) {
		for _, c := range t.closers {
			c.Close()
		}

		t.cancel()

		t.startReadingWg.Wait()
		testhelpers.AlwaysReturn(t.mReader.ReadAtOutput.Ret2, io.EOF)
		close(t.mReader.ReadAtOutput.Ret0)
		close(t.mReader.ReadAtOutput.Ret1)
	})

	o.Group("when fetching the buffer does not return an error", func() {
		o.BeforeEach(func(t TR) TR {
			close(t.mockIOFetcher.FetchReaderOutput.Ret1)
			return t
		})

		o.Group("when reader doesn't return an error", func() {
			o.Group("when start index is 0 (beginning)", func() {
				o.BeforeEach(func(t TR) TR {
					t.reader = setupReader(t.ctx, t.client, t.info)
					close(t.mReader.LastIndexOutput.Ret0)

					return t
				})

				o.Spec("it uses the expected name for the fetcher only once", func(t TR) {
					startReading(t.reader, t.startReadingWg)

					Expect(t, t.mockIOFetcher.FetchReaderInput.Name).To(ViaPolling(HaveLen(1)))
					Expect(t, t.mockIOFetcher.FetchReaderInput.Name).To(Always(HaveLen(1)))
					Expect(t, t.mockIOFetcher.FetchReaderInput.Name).To(
						Chain(Receive(), Equal(t.info.Name)),
					)
				})

				o.Spec("it returns data from the reader", func(t TR) {
					data, _, _ := startReading(t.reader, t.startReadingWg)
					writeToReader(t.mReader, []byte("some-data"), 0, nil)

					Expect(t, data).To(ViaPolling(
						Chain(Receive(), Equal([]byte("some-data"))),
					))
				})

				o.Spec("it returns indexes from the reader", func(t TR) {
					_, indexes, _ := startReading(t.reader, t.startReadingWg)
					writeToReader(t.mReader, []byte("some-data"), 0, nil)
					writeToReader(t.mReader, []byte("some-data"), 1, nil)
					writeToReader(t.mReader, []byte("some-data"), 2, nil)

					Expect(t, indexes).To(ViaPolling(
						Chain(Receive(), Equal(uint64(0))),
					))
					Expect(t, indexes).To(ViaPolling(
						Chain(Receive(), Equal(uint64(1))),
					))
					Expect(t, indexes).To(ViaPolling(
						Chain(Receive(), Equal(uint64(2))),
					))
				})

				o.Spec("it increments the index each read", func(t TR) {
					writeToReader(t.mReader, []byte("some-data-0"), 0, nil)
					writeToReader(t.mReader, []byte("some-data-1"), 1, nil)

					Expect(t, t.mReader.ReadAtInput.Index).To(ViaPolling(
						Chain(Receive(), Equal(uint64(0))),
					))
					Expect(t, t.mReader.ReadAtInput.Index).To(ViaPolling(
						Chain(Receive(), Equal(uint64(1))),
					))
				})

				o.Group("when tailing the reader", func() {
					o.Spec("it waits and then tries again", func(t TR) {
						_, _, errs := startReading(t.reader, t.startReadingWg)
						writeToReader(t.mReader, nil, 0, io.EOF)
						writeToReader(t.mReader, nil, 0, io.EOF)

						Expect(t, t.mReader.ReadAtCalled).To(ViaPolling(HaveLen(3)))
						Expect(t, errs).To(HaveLen(0))
					})
				})
			})

			o.Group("when start index is set to non-zero", func() {
				o.BeforeEach(func(t TR) TR {
					t.info.StartIndex = 1
					t.reader = setupReader(t.ctx, t.client, t.info)

					return t
				})

				o.Spec("it starts at the given index", func(t TR) {
					writeToReader(t.mReader, []byte("some-data-3"), 0, nil)
					writeToReader(t.mReader, []byte("some-data-1"), 1, nil)
					writeToReader(t.mReader, []byte("some-data-2"), 2, nil)

					Expect(t, t.mReader.ReadAtInput.Index).To(ViaPolling(
						Chain(Receive(), Equal(uint64(1))),
					))
				})
			})

			o.Group("when StartFromEnd is set to true", func() {
				o.BeforeEach(func(t TR) TR {
					t.info.StartIndex = 1
					t.info.StartFromEnd = true
					t.mReader.LastIndexOutput.Ret0 <- 2

					t.reader = setupReader(t.ctx, t.client, t.info)
					return t
				})

				o.Spec("it starts from the end", func(t TR) {
					writeToReader(t.mReader, []byte("some-data-3"), 0, nil)
					writeToReader(t.mReader, []byte("some-data-1"), 1, nil)
					writeToReader(t.mReader, []byte("some-data-2"), 2, nil)

					Expect(t, t.mReader.ReadAtInput.Index).To(ViaPolling(
						Chain(Receive(), Equal(uint64(2))),
					))
				})
			})
		})

		o.Group("when the reader returns an error", func() {
			o.BeforeEach(func(t TR) TR {
				t.reader = setupReader(t.ctx, t.client, t.info)
				return t
			})

			o.Spec("it returns an error", func(t TR) {
				_, _, errs := startReading(t.reader, t.startReadingWg)
				writeToReader(t.mReader, nil, 0, fmt.Errorf("some-error"))

				Expect(t, errs).To(ViaPolling(
					Not(HaveLen(0)),
				))
			})
		})
	})

	o.Group("when fetching the buffer returns an error", func() {
		o.BeforeEach(func(t TR) TR {
			t.mockIOFetcher.FetchReaderOutput.Ret1 <- fmt.Errorf("some-error")

			t.reader = setupReader(t.ctx, t.client, t.info)
			return t
		})

		o.Spec("it returns an error", func(t TR) {
			_, _, errs := startReading(t.reader, t.startReadingWg)

			Expect(t, errs).To(ViaPolling(
				Not(HaveLen(0)),
			))
		})
	})

}

type handlerWrapper struct {
	rDone   chan struct{}
	wDone   chan struct{}
	handler pb.NodeServer
}

func newHandlerWrapper(handler pb.NodeServer) *handlerWrapper {
	return &handlerWrapper{
		rDone:   make(chan struct{}),
		wDone:   make(chan struct{}),
		handler: handler,
	}
}

func (w *handlerWrapper) Write(s pb.Node_WriteServer) error {
	defer close(w.wDone)
	return w.handler.Write(s)
}

func (w *handlerWrapper) Read(i *pb.BufferInfo, s pb.Node_ReadServer) error {
	defer close(w.rDone)
	return w.handler.Read(i, s)
}

func setupGrpcServer(handler pb.NodeServer) (net.Listener, *handlerWrapper) {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	gs := grpc.NewServer()
	handlerWrapper := newHandlerWrapper(handler)
	pb.RegisterNodeServer(gs, handlerWrapper)
	go gs.Serve(lis)
	return lis, handlerWrapper
}

func establishClient(URI string) (pb.NodeClient, *grpc.ClientConn) {
	conn, err := grpc.Dial(URI, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	return pb.NewNodeClient(conn), conn
}

func keepWriting(writer pb.Node_WriteClient, p *pb.WriteDataPacket) func() bool {
	return func() bool {
		return writer.Send(p) == nil
	}
}

func setupReader(ctx context.Context, client pb.NodeClient, info *pb.BufferInfo) pb.Node_ReadClient {
	reader, err := client.Read(ctx, info)
	if err != nil {
		panic(err)
	}
	return reader
}

func startReading(reader pb.Node_ReadClient, wg *sync.WaitGroup) (chan []byte, chan uint64, chan error) {
	wg.Add(1)
	d := make(chan []byte, 100)
	i := make(chan uint64, 100)
	e := make(chan error, 100)
	go func(r pb.Node_ReadClient) {
		defer wg.Done()
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

func writeToReader(reader *mockReader, data []byte, idx uint64, err error) {
	reader.ReadAtOutput.Ret0 <- data
	reader.ReadAtOutput.Ret1 <- idx
	reader.ReadAtOutput.Ret2 <- err
}
