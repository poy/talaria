package urifinder

import (
	"context"
	"log"

	"github.com/apoydence/talaria/pb/intra"
	"google.golang.org/grpc"
)

type URIFinder struct {
	client intra.SchedulerClient
}

func New(schedulerURI string) *URIFinder {
	return &URIFinder{
		client: setupClient(schedulerURI),
	}
}

func (f *URIFinder) FromID(ID uint64) (string, error) {
	resp, err := f.client.FromID(context.Background(), &intra.FromIdRequest{
		Id: ID,
	})

	if err != nil {
		return "", err
	}

	return resp.Uri, nil
}

func setupClient(URI string) intra.SchedulerClient {
	conn, err := grpc.Dial(URI, grpc.WithInsecure())
	if err != nil {
		log.Panicf("unable to dial scheduler: %v", err)
	}

	c := intra.NewSchedulerClient(conn)

	return c
}
