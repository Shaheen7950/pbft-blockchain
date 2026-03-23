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

const (
	WeightMin    = 1
	WeightMax    = 10
	RewardDelta  = 1
	PenaltyDelta = 2
)

type PBFT struct {
	ID      string
	Peers   []string
	Weights map[string]int
	LeaderID string

	IsMalicious bool

	PrepareVotes map[string]int
	CommitVotes  map[string]int

	PrepareSeen map[string]map[string]bool
	CommitSeen  map[string]map[string]bool

	PrePrepareSeen map[string]bool

	// canonical hash announced by leader for each "view-seq"
	CanonicalHash map[string]string

	Mutex sync.Mutex
}

func NewPBFT(id string, peers []string, weights map[string]int, malicious bool, leaderID string) *PBFT {
	return &PBFT{
		ID:             id,
		Peers:          peers,
		Weights:        weights,
		LeaderID:	leaderID,
		IsMalicious:    malicious,
		PrepareVotes:   make(map[string]int),
		CommitVotes:    make(map[string]int),
		PrepareSeen:    make(map[string]map[string]bool),
		CommitSeen:     make(map[string]map[string]bool),
		PrePrepareSeen: make(map[string]bool),
		CanonicalHash:  make(map[string]string),
	}
}

func (p *PBFT) adjustWeight(nodeID string, delta int) {
	before := p.Weights[nodeID]
	after := before + delta
	if after < WeightMin {
		after = WeightMin
	}
	if after > WeightMax {
		after = WeightMax
	}
	p.Weights[nodeID] = after
	if before != after {
		dir := "UP"
		if delta < 0 {
			dir = "DOWN"
		}
		fmt.Printf("[Weight] node=%s %s %d -> %d\n", nodeID, dir, before, after)
	}
}

// seqKey identifies a consensus round without the hash, used to look up
// the canonical hash the leader announced in PRE_PREPARE.
func seqKey(view, seq uint32) string {
	return fmt.Sprintf("%d-%d", view, seq)
}

func key(view uint32, seq uint32, hash string) string {
	return fmt.Sprintf("%d-%d-%s", view, seq, hash)
}

func (p *PBFT) HandleMessage(msg *pb.ConsensusMessage) {

	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	sk := seqKey(msg.View, msg.Sequence)

	switch msg.Type {

	// ---------------- PRE_PREPARE ----------------
	case "PRE_PREPARE":

		if msg.Sender != p.LeaderID {
        	fmt.Printf("[Security] Node %s rejected PRE_PREPARE from non-leader %s\n", p.ID, msg.Sender)
        	p.adjustWeight(msg.Sender, -PenaltyDelta)
        	return
    		}

		fmt.Println("Node", p.ID, "received PRE_PREPARE")

		// record canonical hash from the leader
		if _, exists := p.CanonicalHash[sk]; !exists {
			p.CanonicalHash[sk] = msg.BlockHash
		}

		k := key(msg.View, msg.Sequence, msg.BlockHash)
		p.PrePrepareSeen[k] = true

		if p.PrepareSeen[k] == nil {
			p.PrepareSeen[k] = make(map[string]bool)
		}
		if p.CommitSeen[k] == nil {
			p.CommitSeen[k] = make(map[string]bool)
		}

		hash := msg.BlockHash
		if p.IsMalicious {
			fmt.Println("Node", p.ID, "is malicious -> sending fake PREPARE")
			hash = "fake_hash"
		}

		if !p.PrepareSeen[k][p.ID] {
			p.PrepareVotes[k] += p.Weights[p.ID]
			p.PrepareSeen[k][p.ID] = true
		}

		p.broadcast("PREPARE", msg.View, msg.Sequence, hash)

	// ---------------- PREPARE ----------------
	case "PREPARE":

		canonical, known := p.CanonicalHash[sk]
		if !known {
			// PRE_PREPARE not yet received, drop
			return
		}

		// sender's hash doesn't match what the leader announced -> malicious
		if msg.BlockHash != canonical {
			fmt.Printf("[Behavior] Node %s sent wrong PREPARE hash (got %s want %s) -> penalize\n",
				msg.Sender, msg.BlockHash, canonical)
			p.adjustWeight(msg.Sender, -PenaltyDelta)
			return
		}

		k := key(msg.View, msg.Sequence, canonical)

		if p.PrepareSeen[k] == nil {
			p.PrepareSeen[k] = make(map[string]bool)
		}
		if p.CommitSeen[k] == nil {
			p.CommitSeen[k] = make(map[string]bool)
		}

		if p.PrepareSeen[k][msg.Sender] {
			return
		}

		// honest PREPARE
		p.PrepareSeen[k][msg.Sender] = true
		p.PrepareVotes[k] += p.Weights[msg.Sender]
		p.adjustWeight(msg.Sender, RewardDelta)

		fmt.Println("Node", p.ID, "-> Prepare weight:", p.PrepareVotes[k])

		if p.PrepareVotes[k] >= p.quorum() && !p.CommitSeen[k][p.ID] {
			fmt.Println("Node", p.ID, "-> Prepare quorum reached")

			p.CommitVotes[k] += p.Weights[p.ID]
			p.CommitSeen[k][p.ID] = true

			p.broadcast("COMMIT", msg.View, msg.Sequence, canonical)
		}

	// ---------------- COMMIT ----------------
	case "COMMIT":

		canonical, known := p.CanonicalHash[sk]
		if !known {
			return
		}

		if msg.BlockHash != canonical {
			fmt.Printf("[Behavior] Node %s sent wrong COMMIT hash (got %s want %s) -> penalize\n",
				msg.Sender, msg.BlockHash, canonical)
			p.adjustWeight(msg.Sender, -PenaltyDelta)
			return
		}

		k := key(msg.View, msg.Sequence, canonical)

		if p.CommitSeen[k] == nil {
			p.CommitSeen[k] = make(map[string]bool)
		}

		if p.CommitSeen[k][msg.Sender] {
			return
		}

		// honest COMMIT
		p.CommitSeen[k][msg.Sender] = true
		p.CommitVotes[k] += p.Weights[msg.Sender]
		p.adjustWeight(msg.Sender, RewardDelta)

		fmt.Println("Node", p.ID, "-> Commit weight:", p.CommitVotes[k])

		if p.CommitVotes[k] >= p.quorum() {
			fmt.Printf("[FINALIZED] Node %s committed block: %s | weights: %v\n",
				p.ID, msg.BlockHash, p.Weights)
		}
	}
}

func (p *PBFT) quorum() int {
	total := 0
	for _, w := range p.Weights {
		total += w
	}
	return (2*total)/3 + 1
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
			conn, err := grpc.NewClient(addr,
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

// In BroadcastPrePrepare, for malicious nodes, also spam fake PREPAREs
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
            conn, err := grpc.NewClient(addr,
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

            // malicious node also injects a fake PREPARE directly to peers
            if p.IsMalicious {
                fakeMsg := &pb.ConsensusMessage{
                    Type:      "PREPARE",
                    View:      0,
                    Sequence:  seq,
                    BlockHash: "fake_hash",
                    Sender:    p.ID,
                }
                client.SendMessage(ctx, fakeMsg)
            }
        }(peer)
    }
}
