package mail

import (
	"context"
	"fmt"

	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/SJTU-OpenNetwork/hon-textile/keypair"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/service"
	"github.com/golang/protobuf/proto"
	"github.com/ipfs/go-ipfs/core"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/protocol"
	peer "github.com/libp2p/go-libp2p-peer"
)

const mailServiceProtocol = protocol.ID("/hon-textile/mail/1.0.0")


var log = logging.Logger("mailbox")

//var log = logging.Logger("stream")


type MailService struct {
	service *service.Service
	online  bool
	ctx     context.Context
	Inbox   chan *pb.Envelope
}

// NewMailService returns a new mail service
func NewMailService(
	ctx context.Context,
	account *keypair.Full,
	node func() *core.IpfsNode,
) *MailService {
	handler := &MailService{
		ctx:   ctx,
		Inbox: make(chan *pb.Envelope, 10),
	}
	handler.service = service.NewService(account, handler, node)
	return handler
}

// Protocol returns the handler protocol
func (h *MailService) Protocol() protocol.ID {
	return mailServiceProtocol
}

// Start begins online services
func (h *MailService) Start() {
	h.online = true
	h.service.Start()
}

// Ping pings another peer
func (h *MailService) Ping(pid peer.ID) (service.PeerStatus, error) {
	return h.service.Ping(pid.Pretty())
}

// HandleStream is called by the underlying service handler method
func (h *MailService) HandleStream(_ *pb.Envelope, _ peer.ID) (chan *pb.Envelope, chan error, chan interface{}) {
	return make(chan *pb.Envelope), make(chan error), make(chan interface{})
}

// SendMessage sends a message to a peer.
func (h *MailService) SendMessage(ctx context.Context, peerID string, env *pb.Envelope) error {
	//return h.service.SendMessage(ctx, peerID, env)
	log.Debugf("[Mail] Sending message to %s", peerID)
	connected, err := ipfs.SwarmConnected(h.service.Node(), peerID)
	if err != nil {
		return err
	}
	if connected {
		fmt.Println("[Mail]: send message directly")
		err = h.service.SendMessage(nil, peerID, env)
	} else {
		topic := string(mailServiceProtocol) + "/" + peerID
		payload, err := proto.Marshal(env)
		if err != nil {
			return err
		}
		 fmt.Println("[Mail]: send message through pubsub")
		err = ipfs.Publish(h.service.Node(), topic, payload)
	}
	return err
	//return h.service.SendMessage(ctx, peerID, env)
}

// Handle is called by the underlying service handler method
func (h *MailService) Handle(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	// fmt.Println("[Mail]: receive a new message")
	h.Inbox <- env

	return nil, nil
}

func (h *MailService) NewEnvelope(mtype pb.Message_Type, msg proto.Message, id *int32, response bool) (*pb.Envelope, error) {
	return h.service.NewEnvelope(mtype, msg, id, response)
}
