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

func (s *Server) SendMessage(
	ctx context.Context,
	msg *pb.ConsensusMessage,
) (*pb.Ack, error) {

	s.Engine.HandleMessage(msg)

	return &pb.Ack{Success: true}, nil
}
