package main

import (
	"fmt"
	"log"
	"net"

	"github.com/apoydence/talaria/node/config"
	"github.com/apoydence/talaria/node/internal/intraserver"
	"github.com/apoydence/talaria/node/internal/iofetcher"
	"github.com/apoydence/talaria/node/internal/server"
	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	"google.golang.org/grpc"
)

func main() {
	log.Print("Starting Talaria Node...")
	defer log.Print("Closing Talaria Node")
	conf := config.Load()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on port %d", conf.Port)

	ioFetcher := iofetcher.New()
	talaria := server.New(ioFetcher)
	intraServer := intraserver.New(ioFetcher)

	grpcServer := grpc.NewServer()

	intra.RegisterNodeServer(grpcServer, intraServer)
	pb.RegisterTalariaServer(grpcServer, talaria)
	grpcServer.Serve(lis)
}
