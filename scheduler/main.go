package main

import (
	"fmt"
	"log"
	"net"

	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/scheduler/config"
	"github.com/apoydence/talaria/scheduler/internal/nodefetcher"
	"github.com/apoydence/talaria/scheduler/internal/server"
	"google.golang.org/grpc"
)

func main() {
	log.Print("Starting Talaria Scheduler...")
	defer log.Print("Closing Talaria Scheduler")

	conf := config.Load()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on port %d", conf.Port)

	nodeFetcher := nodefetcher.New(conf.Nodes)
	scheduler := server.New(nodeFetcher)

	grpcServer := grpc.NewServer()

	pb.RegisterSchedulerServer(grpcServer, scheduler)
	grpcServer.Serve(lis)
}
