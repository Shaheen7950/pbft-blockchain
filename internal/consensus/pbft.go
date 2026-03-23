package consensus

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "pbft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PBFT struct {
	ID      string
	Peers   []string
	Weights map[string]int

	IsMalicious bool

	PrepareVotes map[string]int
	CommitVotes  map[string]int

	PrepareSeen map[string]map[string]bool
	CommitSeen  map[string]map[string]bool

	PrePrepareSeen map[string]bool

	Mutex sync.Mutex
}

func NewPBFT(id string, peers []string, weights map[string]int, malicious bool) *PBFT {
	return &PBFT{
		ID:             id,
		Peers:          peers,
		Weights:        weights,
		IsMalicious:    malicious,
		PrepareVotes:   make(map[string]int),
		CommitVotes:    make(map[string]int),
		PrepareSeen:    make(map[string]map[string]bool),
		CommitSeen:     make(map[string]map[string]bool),
		PrePrepareSeen: make(map[string]bool),
	}
}

func key(view uint32, seq uint32, hash string) string {
	return fmt.Sprintf("%d-%d-%s", view, seq, hash)
}

func (p *PBFT) HandleMessage(msg *pb.ConsensusMessage) {

	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	k := key(msg.View, msg.Sequence, msg.BlockHash)

	if p.PrepareSeen[k] == nil {
		p.PrepareSeen[k] = make(map[string]bool)
	}
	if p.CommitSeen[k] == nil {
		p.CommitSeen[k] = make(map[string]bool)
	}

	switch msg.Type {

	// ---------------- PRE_PREPARE ----------------
	case "PRE_PREPARE":

		fmt.Println("Node", p.ID, "received PRE_PREPARE")

		p.PrePrepareSeen[k] = true

		hash := msg.BlockHash

		// malicious node sends wrong hash
		if p.IsMalicious {
			fmt.Println("Node", p.ID, "is malicious → sending fake PREPARE")
			hash = "fake_hash"
		}

		// self vote
		if !p.PrepareSeen[k][p.ID] {
			p.PrepareVotes[k] += p.Weights[p.ID]
			p.PrepareSeen[k][p.ID] = true
		}

		p.broadcast("PREPARE", msg.View, msg.Sequence, hash)

	// ---------------- PREPARE ----------------
	case "PREPARE":

		// must have received PRE_PREPARE
		if !p.PrePrepareSeen[k] {
			return
		}

		// reject mismatched hash (malicious)
		if msg.BlockHash != extractHash(k) {
			return
		}

		// avoid duplicate
		if p.PrepareSeen[k][msg.Sender] {
			return
		}

		p.PrepareSeen[k][msg.Sender] = true
		p.PrepareVotes[k] += p.Weights[msg.Sender]

		fmt.Println("Node", p.ID, "→ Prepare weight:", p.PrepareVotes[k])

		// STRICT: only move to COMMIT once
		if p.PrepareVotes[k] >= p.quorum() && !p.CommitSeen[k][p.ID] {

			fmt.Println("Node", p.ID, "→ Prepare quorum reached")

			p.CommitVotes[k] += p.Weights[p.ID]
			p.CommitSeen[k][p.ID] = true

			p.broadcast("COMMIT", msg.View, msg.Sequence, msg.BlockHash)
		}

	// ---------------- COMMIT ----------------
	case "COMMIT":

		if !p.PrePrepareSeen[k] {
			return
		}

		// reject mismatched hash
		if msg.BlockHash != extractHash(k) {
			return
		}

		if p.CommitSeen[k][msg.Sender] {
			return
		}

		p.CommitSeen[k][msg.Sender] = true
		p.CommitVotes[k] += p.Weights[msg.Sender]

		fmt.Println("Node", p.ID, "→ Commit weight:", p.CommitVotes[k])

		if p.CommitVotes[k] >= p.quorum() {
			fmt.Println("🔥 Node", p.ID, "FINALIZED BLOCK:", msg.BlockHash)
		}
	}
}

func extractHash(k string) string {
	var view, seq uint32
	var hash string
	fmt.Sscanf(k, "%d-%d-%s", &view, &seq, &hash)
	return hash
}

func (p *PBFT) quorum() int {
	total := 0
	for _, w := range p.Weights {
		total += w
	}
	return (2 * total) / 3
}

func (p *PBFT) broadcast(msgType string, view uint32, seq uint32, hash string) {

	msg := &pb.ConsensusMessage{
		Type:      msgType,
		View:      view,
		Sequence:  seq,
		BlockHash: hash,
		Sender:    p.ID,
	}

	for _, peer := range p.Peers {

		go func(addr string) {

			conn, err := grpc.NewClient(
				addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				fmt.Println("Client error:", err)
				return
			}
			defer conn.Close()

			client := pb.NewConsensusServiceClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			_, err = client.SendMessage(ctx, msg)
			if err != nil {
				fmt.Println("Send error:", err)
			}

		}(peer)
	}
}

func (p *PBFT) BroadcastPrePrepare(hash string, seq uint32) {

	msg := &pb.ConsensusMessage{
		Type:      "PRE_PREPARE",
		View:      0,
		Sequence:  seq,
		BlockHash: hash,
		Sender:    p.ID,
	}

	p.HandleMessage(msg)

	for _, peer := range p.Peers {

		go func(addr string) {

			conn, err := grpc.NewClient(
				addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				fmt.Println("Client error:", err)
				return
			}
			defer conn.Close()

			client := pb.NewConsensusServiceClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			client.SendMessage(ctx, msg)

		}(peer)
	}
}
