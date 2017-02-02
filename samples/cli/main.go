package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/apoydence/talaria/api/intra"
	pb "github.com/apoydence/talaria/api/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	verbose = flag.Bool("verbose", false, "Verbose mode")

	schedulerUri    = flag.String("scheduler", "", "The URI for the scheduler")
	nodeUri         = flag.String("node", "", "The URI for the node")
	bufferName      = flag.String("buffer", "", "The buffer to interact with")
	writePacketSize = flag.Uint("packetSize", 1024, "The size of each write packet")

	list      = flag.Bool("list", false, "List cluster info")
	create    = flag.Bool("create", false, "Create a buffer")
	tail      = flag.Bool("tail", false, "The buffer to tail")
	writeData = flag.Bool("write", false, "Write data from STDIN")

	diag = flag.Bool("diag", false, "Look at IDs for a buffer on a node")
)

func main() {
	flag.Parse()

	if !*verbose {
		grpclog.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags))
	}

	if *create {
		createCommand()
		return
	}

	if *tail {
		tailCommand()
		return
	}

	if *writeData {
		writeDataCommand()
		return
	}

	if *list {
		listCommand()
		return
	}

	if *diag {
		diagCommand()
		return
	}

	onlyOneCommandUsage()
}

func createCommand() {
	if *tail || *writeData || *list {
		onlyOneCommandUsage()
	}

	if *bufferName == "" {
		log.Fatal("You must provide a buffer name")
	}

	if *schedulerUri == "" {
		log.Fatal("You must provide a scheduler URI")
	}

	if *nodeUri != "" {
		log.Fatal("You can't provide a node URI")
	}

	client := setupSchedulerClient()

	_, err := client.Create(context.Background(), &pb.CreateInfo{Name: *bufferName})
	if err != nil {
		log.Fatalf("unable to create buffer %s: %s", *bufferName, err)
	}
}

func setupSchedulerClient() pb.SchedulerClient {
	conn, err := grpc.Dial(*schedulerUri, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("unable to connect to scheduler: %s", err)
	}
	return pb.NewSchedulerClient(conn)
}

func setupNodeClient(URI string) pb.NodeClient {
	conn, err := grpc.Dial(URI, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("unable to connect to node: %s", err)
	}
	return pb.NewNodeClient(conn)
}

func tailCommand() {
	if *create || *writeData || *list {
		onlyOneCommandUsage()
	}

	if *bufferName == "" {
		log.Fatal("You must provide a buffer name")
	}

	if *schedulerUri != "" {
		log.Fatal("You can't provide a scheduler URI")
	}

	if *nodeUri == "" {
		log.Fatal("You must provide a node URI")
	}

	client := setupNodeClient(*nodeUri)
	rx, err := client.Read(context.Background(), &pb.BufferInfo{Name: *bufferName})
	if err != nil {
		log.Fatal(err)
	}

	for {
		packet, err := rx.Recv()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(packet)
	}
}

func writeDataCommand() {
	if *create || *tail || *list {
		onlyOneCommandUsage()
	}

	if *bufferName == "" {
		log.Fatal("You must provide a buffer name")
	}

	if *schedulerUri == "" && *nodeUri == "" {
		log.Fatal("You must provide a scheduler URI or node URI")
	}

	if *schedulerUri != "" && *nodeUri != "" {
		log.Fatal("You must provide a scheduler URI or node URI (not both)")
	}

	client := fetchWrtierNode()
	tx, err := client.Write(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		// Give the buffer time to clear
		time.Sleep(250 * time.Millisecond)
		tx.CloseSend()
	}()

	var count int
	buffer := make([]byte, *writePacketSize)
	for {
		n, err := os.Stdin.Read(buffer)
		if err == io.EOF {
			resp, _ := tx.CloseAndRecv()
			log.Printf("Done writing (actual=%d expected=%d)", resp.LastWriteIndex, count)
			return
		}

		if err != nil {
			log.Fatal(err)
		}

		err = tx.Send(&pb.WriteDataPacket{
			Name:    *bufferName,
			Message: buffer[:n],
		})

		if err != nil {
			resp, err := tx.CloseAndRecv()
			fmt.Printf("!! %#v\n", resp)
			log.Fatal(err)
		}

		fmt.Println("Wrote", string(buffer[:n]))
		count++
	}

}

func listCommand() {
	if *create || *tail || *writeData {
		onlyOneCommandUsage()
	}

	if *schedulerUri == "" {
		log.Fatal("You must provide a scheduler URI")
	}

	if *nodeUri != "" {
		log.Fatal("You can't provide a node URI")
	}

	client := setupSchedulerClient()

	listInfo := new(pb.ListInfo)
	if *bufferName != "" {
		listInfo.Names = []string{*bufferName}
	}

	resp, err := client.ListClusterInfo(context.Background(), listInfo)
	if err != nil {
		log.Fatalf("unable to list buffer info %s: %s", *bufferName, err)
	}

	if len(resp.Info) == 0 {
		fmt.Println("<NO INFO REPORTED>")
		return
	}

	for _, info := range resp.Info {
		fmt.Println("BUFFER NAME:")
		fmt.Println(info.Name)
		fmt.Println("\nLEADER:")
		if info.Leader == "" {
			info.Leader = "<NO LEADER REPORTED>"
		}
		fmt.Println(info.Leader)

		fmt.Println("\nNODES:")
		for _, n := range info.Nodes {
			fmt.Printf("URI: %s\n", n.URI)
		}
		fmt.Println()
	}
}

func setupIntraNodeClient() intra.NodeClient {
	conn, err := grpc.Dial(*nodeUri, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("unable to connect to intra node: %s", err)
	}
	return intra.NewNodeClient(conn)
}

func diagCommand() {
	c := setupIntraNodeClient()
	resp, err := c.Status(context.Background(), &intra.StatusRequest{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(resp)
}

func onlyOneCommandUsage() {
	log.Fatal("Use only one create, tail, list or write")
}

func fetchWrtierNode() pb.NodeClient {
	if *nodeUri != "" {
		return setupNodeClient(*nodeUri)
	}

	for {
		leader := fetchLeader()
		if leader == "" {
			log.Print("Unable to find leader. Waiting...")
			time.Sleep(time.Second)
			continue
		}

		return setupNodeClient(leader)
	}
}

func fetchLeader() string {
	client := setupSchedulerClient()

	listInfo := &pb.ListInfo{}
	if *bufferName != "" {
		listInfo.Names = []string{*bufferName}
	}

	resp, err := client.ListClusterInfo(context.Background(), listInfo)
	if err != nil {
		log.Fatalf("unable to list buffer info %s: %s", *bufferName, err)
	}

	if len(resp.Info) != 1 {
		return ""
	}

	return resp.Info[0].Leader
}
