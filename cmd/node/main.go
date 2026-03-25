package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"time"

	"pbft/internal/consensus"
	"pbft/internal/network"
	pb "pbft/proto"

	"google.golang.org/grpc"
)

func main() {
	id := os.Getenv("NODE_ID")
	port := os.Getenv("PORT")

	// Auto-generate 15 peers and starting weights
	peers := []string{}
	nodeWeights := make(map[string]int)
	for i := 1; i <= 15; i++ {
		strID := fmt.Sprintf("%d", i)
		if strID != id {
			peerPort := 5000 + i
			peers = append(peers, fmt.Sprintf("node%d:%d", i, peerPort))
		}
		nodeWeights[strID] = 4 // Give everyone equal starting weight of 4
	}

	// REDUCED ATTACK: Only Node 15 is malicious now. The network will survive!
	malicious := (id == "15") 
	roleStr := "Honest"
	if malicious {
		roleStr = "MALICIOUS"
	}

	// Proposer: 1 | Collectors: 1 and 2
	engine := consensus.NewPBFT(id, peers, nodeWeights, malicious, "1", []string{"1", "2"})

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterConsensusServiceServer(grpcServer, &network.Server{Engine: engine})

	log.Printf("2026/03/25 Node %s (Role: %s) listening on %s", id, roleStr, port)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()
// ---------------- SIMULATION LOOP ----------------
	if id == "1" {
		time.Sleep(25 * time.Second) // Let all 15 nodes boot up
		seq := uint32(1)

		for {
			txBatch := engine.GetAndClearMempool()
			if len(txBatch) > 0 {
				
				// --- UPDATED: Format as a real JSON Array ---
				blockData := "["
				for i, tx := range txBatch {
					if i > 0 {
						blockData += ", "
					}
					blockData += fmt.Sprintf(`{"sender": "%s", "receiver": "%s", "amount": %d}`, tx.ClientId, tx.ReceiverId, tx.Amount)
				}
				blockData += "]"
				// --------------------------------------------

				fmt.Printf("\n=== [PROPOSER] Broadcasting Block %d with %d Txs ===\n", seq, len(txBatch))
				engine.BroadcastPrePrepare(blockData, seq)
				seq++

				time.Sleep(5 * time.Second) // Wait for consensus

				// Print weights beautifully for the demo
				fmt.Println("\n>>> [System Snapshot] Network Weights <<<")
				var keys []string
				for k := range engine.Weights {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				fmt.Print("Weights: ")
				for _, k := range keys {
					if k == "1" || k == "2" || k == "11" || k == "15" {
						fmt.Printf("Node %s: %d  |  ", k, engine.Weights[k])
					}
				}
				fmt.Println("\n----------------------------------------------------")
			}
			time.Sleep(2 * time.Second)
		}
	} else if id == "15" {
		time.Sleep(45 * time.Second) // Attack after network is fully stable
		fmt.Println("\n=== [ATTACK] Malicious Node 15 attempting to spoof a PRE_PREPARE ===")
		engine.BroadcastPrePrepare("rogue_block_99", 99)
		select {}
	} else {
		select {} // Followers idle
	}
}
