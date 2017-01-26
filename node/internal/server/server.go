package server

import (
	"fmt"
	"io"
	"log"
	"time"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/stored"
)

type Reader interface {
	ReadAt(index uint64) ([]byte, uint64, error)
	LastIndex() uint64
}

type Writer interface {
	Write(data stored.Data, timeout time.Duration) error
}

type IOFetcher interface {
	FetchWriter(name string) (Writer, error)
	FetchReader(name string) (Reader, error)
	FetchClusters() []string
}

type Server struct {
	fetcher IOFetcher
}

func New(fetcher IOFetcher) *Server {
	return &Server{
		fetcher: fetcher,
	}
}

func (s *Server) ListClusters(ctx context.Context, in *pb.ListClustersInfo) (resp *pb.ListClustersResponse, err error) {
	return &pb.ListClustersResponse{Names: s.fetcher.FetchClusters()}, nil
}

func (s *Server) Write(rx pb.Node_WriteServer) (err error) {
	log.Print("Starting Writer...")
	defer log.Print("Writer done.")

	var writeCount uint64
	defer func() {
		var errMsg string
		if err != nil {
			errMsg = err.Error()
		}
		err = nil

		rx.SendAndClose(&pb.WriteResponse{
			LastWriteIndex: writeCount,
			Error:          errMsg,
		})
	}()

	writers := make(map[string]Writer)
	for {
		packet, err := rx.Recv()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			log.Printf("failed to read from client: %s", err)
			return err
		}

		writer, err := s.fetchWriter(writers, packet.Name)
		if err != nil {
			log.Printf("unknown buffer: '%s'", packet.Name)
			return fmt.Errorf("unknown buffer: '%s'", packet.Name)
		}

		msg := stored.Data{
			Payload: packet.Message,
			Type:    stored.Data_Normal,
		}
		if err = writer.Write(msg, s.getContextDeadline(rx.Context())); err != nil {
			log.Printf("error writing to buffer '%s': %s", packet.Name, err)
			return err
		}
		writeCount++
	}
}

func (s *Server) Read(buffer *pb.BufferInfo, sender pb.Node_ReadServer) error {
	log.Printf("Starting reader for '%s'...", buffer.Name)
	defer log.Printf("Reader done for '%s'.", buffer.Name)

	reader, err := s.fetcher.FetchReader(buffer.Name)
	if err != nil {
		log.Printf("unknown buffer: '%s'", buffer.Name)
		return fmt.Errorf("unknown buffer: '%s'", buffer.Name)
	}

	idx := buffer.StartIndex
	if buffer.StartFromEnd {
		idx = reader.LastIndex()
	}

	for {
		data, actualIdx, err := reader.ReadAt(idx)

		if err == io.EOF && s.isDone(sender.Context()) {
			return io.EOF
		}

		if err == io.EOF {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if err != nil {
			log.Printf("failed to read from '%s': %s", buffer.Name, err)
			return err
		}
		idx++

		err = sender.Send(&pb.ReadDataPacket{
			Message: data,
			Index:   actualIdx,
		})

		if err != nil {
			log.Printf("failed to read: %s", err)
			return err
		}
	}
}

func (s *Server) getContextDeadline(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}

	return deadline.Sub(time.Now())
}

func (s *Server) isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func (s *Server) fetchWriter(m map[string]Writer, name string) (Writer, error) {
	if writer, ok := m[name]; ok {
		return writer, nil
	}

	writer, err := s.fetcher.FetchWriter(name)
	if err != nil {
		return nil, err
	}

	m[name] = writer
	return writer, nil
}
