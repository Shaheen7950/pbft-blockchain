package consensus

import (
	"context"
	"fmt"
	"sync"

	pb "pbft/proto"

	"google.golang.org/grpc"
)

type PBFT struct {
	ID      string
	Peers   []string
	Weights map[string]int

	IsMalicious bool

	View     uint32
	Sequence uint32

	PrepareVotes map[string]int
	CommitVotes  map[string]int

	Mutex sync.Mutex
}

func NewPBFT(id string, peers []string, weights map[string]int, malicious bool) *PBFT {

	return &PBFT{
		ID:           id,
		Peers:        peers,
		Weights:      weights,
		IsMalicious:  malicious,
		PrepareVotes: make(map[string]int),
		CommitVotes:  make(map[string]int),
	}
}

func key(view uint32, seq uint32, hash string) string {
	return fmt.Sprintf("%d-%d-%s", view, seq, hash)
}

func (p *PBFT) HandleMessage(msg *pb.ConsensusMessage) {

	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	// malicious behaviour
	if p.IsMalicious {
		fmt.Println("Node", p.ID, "is malicious. Sending incorrect vote.")
		msg.BlockHash = "fake_hash"
	}

	switch msg.Type {

	case "PRE_PREPARE":

		fmt.Println("Node", p.ID, "received PRE_PREPARE")

		k := key(msg.View, msg.Sequence, msg.BlockHash)

		// count own vote
		p.PrepareVotes[k] += p.Weights[p.ID]

		p.broadcast("PREPARE", msg.View, msg.Sequence, msg.BlockHash)

	case "PREPARE":

		k := key(msg.View, msg.Sequence, msg.BlockHash)

		p.PrepareVotes[k] += p.Weights[msg.Sender]

		fmt.Println("Prepare weight:", p.PrepareVotes[k])

		if p.PrepareVotes[k] >= p.quorum() {

			fmt.Println("Prepare quorum reached")

			p.CommitVotes[k] += p.Weights[p.ID]

			p.broadcast("COMMIT", msg.View, msg.Sequence, msg.BlockHash)
		}

	case "COMMIT":

		k := key(msg.View, msg.Sequence, msg.BlockHash)

		p.CommitVotes[k] += p.Weights[msg.Sender]

		fmt.Println("Commit weight:", p.CommitVotes[k])

		if p.CommitVotes[k] >= p.quorum() {

			fmt.Println("BLOCK FINALIZED:", msg.BlockHash)
		}
	}
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

	// process locally
	go p.HandleMessage(msg)

	for _, peer := range p.Peers {

		go func(addr string) {

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				return
			}

			defer conn.Close()

			client := pb.NewConsensusServiceClient(conn)

			client.SendMessage(context.Background(), msg)

		}(peer)
	}
}
