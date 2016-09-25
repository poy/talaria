package server

import (
	"fmt"
	"io"
	"log"

	"golang.org/x/net/context"

	"github.com/apoydence/talaria/pb"
)

type Reader interface {
	ReadAt(index uint64) ([]byte, uint64, error)
	LastIndex() uint64
}

type Writer interface {
	WriteTo(data []byte) (uint64, error)
}

type IOFetcher interface {
	Create(name string) error
	FetchWriter(name string) (Writer, error)
	FetchReader(name string) (Reader, error)
}

type Server struct {
	fetcher IOFetcher
}

func New(fetcher IOFetcher) *Server {
	return &Server{
		fetcher: fetcher,
	}
}

func (s *Server) Create(ctx context.Context, info *pb.File) (*pb.CreateResponse, error) {
	return new(pb.CreateResponse), s.fetcher.Create(info.FileName)
}

func (s *Server) Write(rx pb.Talaria_WriteServer) error {
	log.Print("Starting Writer...")
	defer log.Print("Writer done.")

	writers := make(map[string]Writer)
	for {
		packet, err := rx.Recv()
		if err != nil {
			log.Printf("failed to read from client: %s", err)
			return err
		}

		writer, err := s.fetchWriter(writers, packet.FileName)
		if err != nil {
			log.Printf("unknown file: '%s'", packet.FileName)
			return fmt.Errorf("unknown file: '%s'", packet.FileName)
		}

		if _, err = writer.WriteTo(packet.Message); err != nil {
			log.Printf("error writing to file '%s': %s", packet.FileName, err)
			return err
		}
	}
}

func (s *Server) Read(file *pb.File, sender pb.Talaria_ReadServer) error {
	log.Printf("Starting reader for '%s'...", file.FileName)
	defer log.Printf("Reader done for '%s'.", file.FileName)

	reader, err := s.fetcher.FetchReader(file.FileName)
	if err != nil {
		log.Printf("unknown file: '%s'", file.FileName)
		return fmt.Errorf("unknown file: '%s'", file.FileName)
	}

	idx := file.StartIndex
	if file.StartFromEnd {
		idx = reader.LastIndex()
	}

	for {
		data, actualIdx, err := reader.ReadAt(idx)

		if err == io.EOF {
			continue
		}

		if err != nil {
			log.Printf("failed to read from '%s': %s", file.FileName, err)
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

func (s *Server) Info(context.Context, *pb.File) (*pb.InfoResponse, error) {
	panic("Not implemented")
	return nil, nil
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
