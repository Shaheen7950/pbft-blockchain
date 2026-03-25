package main

import (
	"context"
	"log"
	"time"

	pb "pbft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:5001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewConsensusServiceClient(conn)

	log.Println("Submitting batch of 4 transactions to Node 1 Mempool...")

	txs := []*pb.Transaction{
		{ClientId: "Professor Smith", ReceiverId: "University", Amount: 500},
		{ClientId: "Alice", ReceiverId: "Bob", Amount: 50},
		{ClientId: "Charlie", ReceiverId: "Dave", Amount: 15},
		{ClientId: "Eve", ReceiverId: "Frank", Amount: 100},
	}

	for _, tx := range txs {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := client.SubmitTransaction(ctx, tx)
		cancel()

		if err != nil {
			log.Printf("Could not submit: %v", err)
		} else {
			log.Printf("Success! %s", res.Message)
		}
		// Pause for 1 second so the professors can watch the Mempool queue them live
		time.Sleep(1 * time.Second) 
	}
}
