package main

import (
	"fmt"
	"log"
	"net"

	"github.com/apoydence/talaria/internal/config"
	"github.com/apoydence/talaria/internal/iofetcher"
	"github.com/apoydence/talaria/internal/server"
	"github.com/apoydence/talaria/pb"
	"google.golang.org/grpc"
)

func main() {
	log.Print("Starting Talaria...")
	defer log.Print("Closing Talaria")
	conf := config.Load()
	fmt.Println(conf)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on port %d", conf.Port)

	ioFetcher := iofetcher.New()
	talaria := server.New(ioFetcher)

	grpcServer := grpc.NewServer()
	pb.RegisterTalariaServer(grpcServer, talaria)
	grpcServer.Serve(lis)
}
