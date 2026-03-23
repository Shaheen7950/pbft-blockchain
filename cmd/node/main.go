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

	// ---------------- ENV ----------------
	id := os.Getenv("NODE_ID")
	port := os.Getenv("PORT")
	peerEnv := os.Getenv("PEERS")

	peers := []string{}
	if peerEnv != "" {
		peers = strings.Split(peerEnv, ",")
	}

	// ---------------- WEIGHTS ----------------
	nodeWeights := map[string]int{
		"1": 4,
		"2": 3,
		"3": 2,
		"4": 1,
	}

	// ---------------- MALICIOUS NODE ----------------
	malicious := false
	if id == "4" {
		malicious = true
	}

	// ---------------- PBFT ENGINE ----------------
	engine := consensus.NewPBFT(id, peers, nodeWeights, malicious)

	// ---------------- SERVER ----------------
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

	// ---------------- LEADER LOGIC ----------------
	go func() {

		time.Sleep(5 * time.Second)

		if id == "1" {

			// -------- TRANSACTION 1 --------
			log.Println("Transaction 1: Normal")
			engine.BroadcastPrePrepare("block123", 1)

			time.Sleep(6 * time.Second)

			// -------- TRANSACTION 2 --------
			log.Println("Transaction 2: Malicious Scenario")
			engine.BroadcastPrePrepare("block456", 2)
		}

	}()

	// ---------------- START SERVER ----------------
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
