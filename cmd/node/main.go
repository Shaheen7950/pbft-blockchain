package main

import (
	"log"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"

	"pbft/internal/consensus"
	"pbft/internal/network"
	pb "pbft/proto"
)

func main() {

	id := os.Getenv("NODE_ID")
	port := os.Getenv("PORT")

	peerEnv := os.Getenv("PEERS")

	peers := []string{}

	if peerEnv != "" {
		peers = strings.Split(peerEnv, ",")
	}

	nodeWeights := map[string]int{
		"1": 4,
		"2": 3,
		"3": 2,
		"4": 1,
	}

	malicious := false

	if id == "4" {
		malicious = true
	}

	engine := consensus.NewPBFT(id, peers, nodeWeights, malicious)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterConsensusServiceServer(
		grpcServer,
		&network.Server{Engine: engine},
	)

	log.Println("Node", id, "listening on", port)

	// leader starts consensus
	go func() {

		time.Sleep(5 * time.Second)

		if id == "1" {

			log.Println("Leader initiating consensus")

			engine.HandleMessage(&pb.ConsensusMessage{
				Type:      "PRE_PREPARE",
				View:      0,
				Sequence:  1,
				BlockHash: "block123",
				Sender:    "1",
			})
		}

	}()

	grpcServer.Serve(lis)
}
