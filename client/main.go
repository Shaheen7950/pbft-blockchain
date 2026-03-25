package main

import (
    "bufio"
    "context"
    "fmt"
    "log"
    "os"
    "strconv"
    "strings"
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
    reader := bufio.NewReader(os.Stdin)

    fmt.Println("=========================================")
    fmt.Println("   Interactive PBFT Transaction Client   ")
    fmt.Println("=========================================")
    fmt.Println("Connected to Node 1 Mempool.")
    fmt.Println("Type 'exit' as the Sender Name to stop.")
    fmt.Println("-----------------------------------------")

    for {
        // 1. Get Sender
        fmt.Print("\nEnter Sender Name: ")
        sender, _ := reader.ReadString('\n')
        sender = strings.TrimSpace(sender)

        // Check if user wants to quit
        if strings.ToLower(sender) == "exit" || strings.ToLower(sender) == "quit" {
            fmt.Println("Exiting client...")
            break
        }

        // 2. Get Receiver
        fmt.Print("Enter Receiver Name: ")
        receiver, _ := reader.ReadString('\n')
        receiver = strings.TrimSpace(receiver)

        // 3. Get Amount
        fmt.Print("Enter Amount: ")
        amountStr, _ := reader.ReadString('\n')
        amountStr = strings.TrimSpace(amountStr)

        // Convert string amount to integer
        amount, err := strconv.Atoi(amountStr)
        if err != nil {
            fmt.Println("Invalid amount. Please enter a valid number.")
            continue
        }

        // Build the transaction
        tx := &pb.Transaction{
            ClientId:   sender,
            ReceiverId: receiver,
            Amount:     int32(amount), // Ensure this matches your protobuf definition (usually int32 or int64)
        }

        // Send to Node 1
        ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
        res, err := client.SubmitTransaction(ctx, tx)
        cancel()

        if err != nil {
            log.Printf("Could not submit: %v", err)
        } else {
            log.Printf("Success %s", res.Message)
        }
    }
}


