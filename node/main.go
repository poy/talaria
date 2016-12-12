package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/apoydence/talaria/node/config"
	"github.com/apoydence/talaria/node/internal/server"
	"github.com/apoydence/talaria/node/internal/storage"
	"github.com/apoydence/talaria/node/internal/storage/intraserver"
	"github.com/apoydence/talaria/node/internal/storage/intraserver/router"
	"github.com/apoydence/talaria/node/internal/storage/urifinder"
	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	"google.golang.org/grpc"
)

func main() {
	seed := time.Now().UnixNano()
	log.Print("Starting Talaria Node...")
	rand.Seed(seed)
	defer log.Print("Closing Talaria Node")
	conf := config.Load()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on port %d", conf.Port)

	ID := uint64(rand.Int63())
	log.Printf("Node ID=%d", ID)

	uriFinder := urifinder.New(conf.SchedulerURI)
	router := router.New()
	store := storage.New(ID, router, uriFinder)
	talaria := server.New(store)
	intraServer := intraserver.New(ID, store, router)

	grpcServer := grpc.NewServer()

	intra.RegisterNodeServer(grpcServer, intraServer)
	pb.RegisterTalariaServer(grpcServer, talaria)
	grpcServer.Serve(lis)
}
