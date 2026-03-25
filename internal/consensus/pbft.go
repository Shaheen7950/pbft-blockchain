package consensus

import (
"context"
"fmt"
"sort"
"os"
"strings"
"sync"
"time"

pb "pbft/proto"

"google.golang.org/grpc"
"google.golang.org/grpc/credentials/insecure"
)

const (
WeightMin    = 0
RewardDelta  = 1
PenaltyDelta = 2
)

type PBFT struct {
ID         string
Peers      []string
Weights    map[string]int
ProposerID string
Collectors map[string]bool

IsMalicious bool

PrepareVotes map[string]int
CommitVotes  map[string]int

PrepareSeen    map[string]map[string]bool
CommitSeen     map[string]map[string]bool
PrePrepareSeen map[string]bool
CanonicalHash  map[string]string

PrepareSigs map[string]map[string]int64
CommitSigs  map[string]map[string]int64

PendingWeights   map[string]map[string]int
IsFinalized      map[string]bool
AggregatedSent   map[string]bool
AggPrepProcessed map[string]bool
AggComProcessed  map[string]bool

Mempool      []*pb.Transaction
MempoolMutex sync.Mutex
Mutex        sync.Mutex
}

func NewPBFT(id string, peers []string, weights map[string]int, malicious bool, proposer string, collectors []string) *PBFT {
colMap := make(map[string]bool)
for _, c := range collectors {
colMap[c] = true
}

return &PBFT{
ID:               id,
Peers:            peers,
Weights:          weights,
ProposerID:       proposer,
Collectors:       colMap,
IsMalicious:      malicious,
PrepareVotes:     make(map[string]int),
CommitVotes:      make(map[string]int),
PrepareSeen:      make(map[string]map[string]bool),
CommitSeen:       make(map[string]map[string]bool),
PrePrepareSeen:   make(map[string]bool),
CanonicalHash:    make(map[string]string),
PrepareSigs:      make(map[string]map[string]int64),
CommitSigs:       make(map[string]map[string]int64),
PendingWeights:   make(map[string]map[string]int),
IsFinalized:      make(map[string]bool),
AggregatedSent:   make(map[string]bool),
AggPrepProcessed: make(map[string]bool),
AggComProcessed:  make(map[string]bool),
Mempool:          make([]*pb.Transaction, 0),
}
}

// MEMPOOL FUNCTIONS
func (p *PBFT) AddTransaction(tx *pb.Transaction) {
p.MempoolMutex.Lock()
defer p.MempoolMutex.Unlock()
p.Mempool = append(p.Mempool, tx)
fmt.Printf("[MEMPOOL] Node %s received Tx: %s sends %d to %s\n", p.ID, tx.ClientId, tx.Amount, tx.ReceiverId)
}

func (p *PBFT) GetAndClearMempool() []*pb.Transaction {
p.MempoolMutex.Lock()
defer p.MempoolMutex.Unlock()
batch := make([]*pb.Transaction, len(p.Mempool))
copy(batch, p.Mempool)
p.Mempool = make([]*pb.Transaction, 0)
return batch
}

// CORE PBFT LOGIC
func (p *PBFT) adjustWeight(nodeID string, delta int) {
before := p.Weights[nodeID]
after := before + delta
if after < WeightMin {
after = WeightMin
}

totalWeight := 0
for _, w := range p.Weights {
totalWeight += w
}

maxAllowedWeight := totalWeight / 3
if maxAllowedWeight < 1 {
maxAllowedWeight = 1
}

if after > maxAllowedWeight {
after = maxAllowedWeight
}

p.Weights[nodeID] = after

if before != after {
direction := "UP"
if after < before {
direction = "DOWN"
}

reason := "Reward"
if delta < 0 {
reason = "Penalty"
} else if delta > 0 && after < before {
reason = "Network Cap Adjustment"
}

if nodeID == "15" || p.Collectors[nodeID] {
fmt.Printf("[Weight] node=%s %s %d -> %d (%s, Max Cap: %d)\n", nodeID, direction, before, after, reason, maxAllowedWeight)
}
}
if after == 0 && before > 0 {
fmt.Printf("[Weight] node=%s has reached ZERO voting power\n", nodeID)
}
}

func seqKey(view, seq uint32) string { return fmt.Sprintf("%d-%d", view, seq) }
func key(view uint32, seq uint32, hash string) string { return fmt.Sprintf("%d-%d-%s", view, seq, hash) }

func (p *PBFT) HandleMessage(msg *pb.ConsensusMessage) {
p.Mutex.Lock()
defer p.Mutex.Unlock()

sk := seqKey(msg.View, msg.Sequence)
if p.PendingWeights[sk] == nil {
p.PendingWeights[sk] = make(map[string]int)
}

switch msg.Type {

case "PRE_PREPARE":
if msg.Sender != p.ProposerID {
if p.Collectors[p.ID] {
fmt.Printf("[Security] Node %s rejected rogue PRE_PREPARE from %s\n", p.ID, msg.Sender)
}
p.adjustWeight(msg.Sender, -PenaltyDelta)
return
}

if _, exists := p.CanonicalHash[sk]; !exists {
p.CanonicalHash[sk] = msg.BlockHash
}

hash := msg.BlockHash
if p.IsMalicious {
hash = "fake_hash"
}

mySig := Sign(p.ID, "PREPARE:"+hash)
p.broadcast("PREPARE", msg.View, msg.Sequence, hash, mySig, nil)

go p.HandleMessage(&pb.ConsensusMessage{
Type: "PREPARE", View: msg.View, Sequence: msg.Sequence, BlockHash: hash, Sender: p.ID, Signature: mySig,
})

case "PREPARE":
canonical, known := p.CanonicalHash[sk]
if !known {
return
}

if msg.BlockHash != canonical {
if !(msg.Sender == p.ID && p.IsMalicious) {
p.PendingWeights[sk][msg.Sender] = -PenaltyDelta
}
return
}

if p.PendingWeights[sk][msg.Sender] >= 0 {
p.PendingWeights[sk][msg.Sender] = RewardDelta
}

if !p.Collectors[p.ID] {
return
}

if !VerifySingle(msg.Sender, "PREPARE:"+msg.BlockHash, msg.Signature) {
return
}

k := key(msg.View, msg.Sequence, canonical)
if p.PrepareSeen[k] == nil {
p.PrepareSeen[k] = make(map[string]bool)
p.PrepareSigs[k] = make(map[string]int64)
}
if p.PrepareSeen[k][msg.Sender] {
return
}

p.PrepareSeen[k][msg.Sender] = true
p.PrepareVotes[k] += p.Weights[msg.Sender]
p.PrepareSigs[k][msg.Sender] = msg.Signature

fmt.Printf("[Collector %s] -> Received PREPARE from Node %s | Total Weight: %d/%d\n", p.ID, msg.Sender, p.PrepareVotes[k], p.quorum())
aggrKey := "prep_" + k
if p.PrepareVotes[k] >= p.quorum() && !p.AggregatedSent[aggrKey] {
p.AggregatedSent[aggrKey] = true
var sigs []int64
var signers []string
for nodeID, sig := range p.PrepareSigs[k] {
sigs = append(sigs, sig)
signers = append(signers, nodeID)
}
aggSig := AggregateSignatures(sigs)
fmt.Printf("=== [Collector %s] QUORUM REACHED! Aggregating %d PREPARE signatures ===\n", p.ID, len(sigs))
p.broadcast("AGGREGATED_PREPARE", msg.View, msg.Sequence, canonical, aggSig, signers)

go p.HandleMessage(&pb.ConsensusMessage{
Type: "AGGREGATED_PREPARE", View: msg.View, Sequence: msg.Sequence, BlockHash: canonical, Sender: p.ID, Signature: aggSig, Signers: signers,
})
}

case "AGGREGATED_PREPARE":
if !p.Collectors[msg.Sender] {
return
}
if p.AggPrepProcessed[sk] {
return
}
if !VerifyAggregated(msg.Signers, "PREPARE:"+msg.BlockHash, msg.Signature) {
return
}
p.AggPrepProcessed[sk] = true

hash := msg.BlockHash
if p.IsMalicious {
hash = "fake_hash"
}

mySig := Sign(p.ID, "COMMIT:"+hash)
p.broadcast("COMMIT", msg.View, msg.Sequence, hash, mySig, nil)

go p.HandleMessage(&pb.ConsensusMessage{
Type: "COMMIT", View: msg.View, Sequence: msg.Sequence, BlockHash: hash, Sender: p.ID, Signature: mySig,
})

case "COMMIT":
canonical, known := p.CanonicalHash[sk]
if !known {
return
}

if msg.BlockHash != canonical {
if !(msg.Sender == p.ID && p.IsMalicious) {
p.PendingWeights[sk][msg.Sender] = -PenaltyDelta
}
return
}

if p.PendingWeights[sk][msg.Sender] >= 0 {
p.PendingWeights[sk][msg.Sender] = RewardDelta
}

if !p.Collectors[p.ID] {
return
}

if !VerifySingle(msg.Sender, "COMMIT:"+msg.BlockHash, msg.Signature) {
return
}

k := key(msg.View, msg.Sequence, canonical)
if p.CommitSeen[k] == nil {
p.CommitSeen[k] = make(map[string]bool)
p.CommitSigs[k] = make(map[string]int64)
}
if p.CommitSeen[k][msg.Sender] {
return
}

p.CommitSeen[k][msg.Sender] = true
p.CommitVotes[k] += p.Weights[msg.Sender]
p.CommitSigs[k][msg.Sender] = msg.Signature

fmt.Printf("[Collector %s] -> Received COMMIT from Node %s | Total Weight: %d/%d\n", p.ID, msg.Sender, p.CommitVotes[k], p.quorum())

aggrKey := "com_" + k
if p.CommitVotes[k] >= p.quorum() && !p.AggregatedSent[aggrKey] {
p.AggregatedSent[aggrKey] = true
var sigs []int64
var signers []string
for nodeID, sig := range p.CommitSigs[k] {
sigs = append(sigs, sig)
signers = append(signers, nodeID)
}
aggSig := AggregateSignatures(sigs)
fmt.Printf("=== [Collector %s] QUORUM REACHED! Aggregating %d COMMIT signatures ===\n", p.ID, len(sigs))
p.broadcast("AGGREGATED_COMMIT", msg.View, msg.Sequence, canonical, aggSig, signers)

go p.HandleMessage(&pb.ConsensusMessage{
Type: "AGGREGATED_COMMIT", View: msg.View, Sequence: msg.Sequence, BlockHash: canonical, Sender: p.ID, Signature: aggSig, Signers: signers,
})
}

case "AGGREGATED_COMMIT":
if !p.Collectors[msg.Sender] {
return
}
if p.AggComProcessed[sk] {
return
}
if !VerifyAggregated(msg.Signers, "COMMIT:"+msg.BlockHash, msg.Signature) {
return
}
p.AggComProcessed[sk] = true

if !p.IsFinalized[sk] {
p.IsFinalized[sk] = true

if p.ID == p.ProposerID {
fmt.Printf("\n[FINALIZED] Block Committed: %s\n", msg.BlockHash)
}

p.saveToLedger(msg.Sequence, msg.BlockHash)

var nodes []string
for nodeID := range p.Weights {
nodes = append(nodes, nodeID)
}
sort.Strings(nodes)

for _, nodeID := range nodes {
if delta, exists := p.PendingWeights[sk][nodeID]; exists {
p.adjustWeight(nodeID, delta)
}
}
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

// O(N) STAR TOPOLOGY ROUTING APPLIED
func (p *PBFT) broadcast(msgType string, view uint32, seq uint32, hash string, sig int64, signers []string) {
msg := &pb.ConsensusMessage{
Type: msgType, View: view, Sequence: seq, BlockHash: hash, Sender: p.ID, Signature: sig, Signers: signers,
}
for _, peer := range p.Peers {
peerID := strings.TrimPrefix(strings.Split(peer, ":")[0], "node")

// If this is a vote, ONLY send it to the Collectors. Ignore everyone else.
if (msgType == "PREPARE" || msgType == "COMMIT") && !p.Collectors[peerID] {
continue
}

go func(addr string) {
conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
return
}
defer conn.Close()
client := pb.NewConsensusServiceClient(conn)

// 5 SECOND TIMEOUT TO PREVENT DOCKER NETWORK DROPS
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

client.SendMessage(ctx, msg)
}(peer)
}
}

func (p *PBFT) BroadcastPrePrepare(hash string, seq uint32) {
msg := &pb.ConsensusMessage{Type: "PRE_PREPARE", View: 0, Sequence: seq, BlockHash: hash, Sender: p.ID}
p.HandleMessage(msg)
for _, peer := range p.Peers {
go func(addr string) {
conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
return
}
defer conn.Close()
client := pb.NewConsensusServiceClient(conn)

// 5 SECOND TIMEOUT TO PREVENT DOCKER NETWORK DROPS
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

client.SendMessage(ctx, msg)
}(peer)
}
}

// --- STORAGE LAYER ---
// Appends finalized blocks to a JSON file on the local disk
func (p *PBFT) saveToLedger(seq uint32, blockData string) {
	filename := fmt.Sprintf("ledger_node_%s.json", p.ID)
	
	// UPDATED: Removed the quotes around %s for transactions so it saves as an actual JSON array
	entry := fmt.Sprintf("{\"sequence\": %d, \"timestamp\": \"%s\", \"transactions\": %s}\n", 
		seq, time.Now().Format(time.RFC3339), blockData)
		
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("[STORAGE ERROR] Node %s could not write to ledger\n", p.ID)
		return
	}
	defer file.Close()
	
	file.WriteString(entry)
	
	// Print a clean message to the terminal to show the data was saved
	if p.ID == "1" || p.ID == "2" {
		fmt.Printf("[STORAGE LAYER] Node %s permanently appended Block %d to %s\n", p.ID, seq, filename)
	}
}
