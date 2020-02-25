// Service for sending/receving stream related data - add by Jerry 2020/02/25

package core

import (
//	"bytes"
	"strings"
	"context"
//	"encoding/base64"
//	"fmt"
//	"time"

//	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/SJTU-OpenNetwork/go-ipfs/core"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
//	mh "github.com/multiformats/go-multihash"
//	"github.com/segmentio/ksuid"
//	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
//	"github.com/SJTU-OpenNetwork/hon-textile/crypto"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/SJTU-OpenNetwork/hon-textile/keypair"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
//	"github.com/SJTU-OpenNetwork/hon-textile/repo/db"
	"github.com/SJTU-OpenNetwork/hon-textile/service"
)


// streamServiceProtocol is the current protocol tag
const streamServiceProtocol = protocol.ID("/textile/stream/1.0.0")

type StreamService struct {
	service          *service.Service
	datastore        repo.Datastore
	online           bool
}

// NewStreamService returns a new stream service
func NewStreamService(
	account *keypair.Full,
	node func() *core.IpfsNode,
	datastore repo.Datastore,
) *StreamService {
	handler := &StreamService{
		datastore:        datastore,
	}
	handler.service = service.NewService(account, handler, node)
	return handler
}

// Protocol returns the handler protocol
func (h *StreamService) Protocol() protocol.ID {
	return streamServiceProtocol
}

// Start begins online services
func (h *StreamService) Start() {
	h.service.Start()
}

// Ping pings another peer
func (h *StreamService) Ping(pid peer.ID) (service.PeerStatus, error) {
	return h.service.Ping(pid.Pretty())
}

// handleStreamBlock receives a STREAMBLOCK message
func (h *StreamService) handleStreamBlock(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
    block := new(pb.StreamBlockContent)
    err := ptypes.UnmarshalAny(env.Message.Payload, block)
    if err != nil {
        return nil, err
    }
    _, err = ipfs.PutBlock(h.service.Node(), strings.NewReader(block.Data))
    return nil, err
}

// Handle is called by the underlying service handler method
func (h *StreamService) Handle(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	switch env.Message.Type {
	case pb.Message_STREAM_BLOCK:
		return h.handleStreamBlock(env, pid)
    default:
        return nil, nil
    }
}

// HandleStream is called by the underlying service handler method
func (h *StreamService) HandleStream(env *pb.Envelope, pid peer.ID) (chan *pb.Envelope, chan error, chan interface{}) {
	// no-op
	return make(chan *pb.Envelope), make(chan error), make(chan interface{})
}

// SendMessage sends a message to a peer
func (h *StreamService) SendMessage(ctx context.Context, peerId string, env *pb.Envelope) error {
	return h.service.SendMessage(ctx, peerId, env)
}
