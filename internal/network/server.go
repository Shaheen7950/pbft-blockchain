package network

import (
	"context"

	"pbft/internal/consensus"
	pb "pbft/proto"
)

type Server struct {
	pb.UnimplementedConsensusServiceServer
	Engine *consensus.PBFT
}

func (s *Server) SendMessage(ctx context.Context, msg *pb.ConsensusMessage) (*pb.Empty, error) {
	go s.Engine.HandleMessage(msg)
	return &pb.Empty{}, nil
}

func (s *Server) SubmitTransaction(ctx context.Context, tx *pb.Transaction) (*pb.TxAck, error) {
	s.Engine.AddTransaction(tx)
	return &pb.TxAck{
		Success: true,
		Message: "Transaction queued in Mempool",
	}, nil
}
