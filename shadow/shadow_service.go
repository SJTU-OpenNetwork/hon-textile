// Service for sending/receving messages to the shadow node - add by Jerry 2020.04.05

package shadow

import (
	"fmt"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/SJTU-OpenNetwork/hon-textile/keypair"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	ma "github.com/multiformats/go-multiaddr"

	//	"github.com/SJTU-OpenNetwork/hon-textile/repo/db"
	"github.com/SJTU-OpenNetwork/hon-textile/service"
	"github.com/SJTU-OpenNetwork/go-ipfs/core"
)


// streamServiceProtocol is the current protocol tag
const shadowServiceProtocol = protocol.ID("/textile/shadow/1.0.0")

type ShadowService struct {
	service          *service.Service
	datastore        repo.Datastore
	online           bool
	sendNotification func(*pb.Notification) error
	isShadow		 bool
}

func NewShadowService(
	account *keypair.Full,
	node func() *core.IpfsNode,
	datastore repo.Datastore,
	sendNotification func(*pb.Notification) error,
	isShadow bool,
) *ShadowService {
	handler := &ShadowService{
		datastore:        datastore,
		sendNotification: sendNotification,
		isShadow:		  isShadow,
	}
	handler.service = service.NewService(account, handler, node)
	return handler
}

// Protocol returns the handler protocol
func (h *ShadowService) Protocol() protocol.ID {
	return shadowServiceProtocol
}

// Start begins online services
func (h *ShadowService) Start() {
	h.online = true
	h.service.Start()
	h.service.Node().PeerHost.Network().Notify((*ShadowNotifee)(h))
}

// Handle is called by the underlying service handler method
func (h *ShadowService) Handle(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	fmt.Printf("core/stream_service.go Handler: New message receive from %s.\n", pid.Pretty())
	switch env.Message.Type {
	case pb.Message_SHADOW_INFORM:
		return h.handleInform(env, pid)
	case pb.Message_SHADOW_STREAM_META:
		return h.handleStreamMeta(env, pid)
    default:
        return nil, nil
    }
}

// TODO: if the shadow node is disconnected, modify the work mode
func (h *ShadowService) PeerDisconnected(pid peer.ID) error{
	return nil
}

// TODO: automatically connect to the shadow node
// 		It informs the remote peer that "Here is a shadow peer."
// 		Avoid to call it multi time for the same peer!!
func (h *ShadowService) PeerConnected(pid peer.ID, multiaddr ma.Multiaddr) error{
    if h.isShadow {
    	err := h.inform(pid); if err != nil {return err}
	}
    return nil
}

// TODO: inform pid about my information (e.g., public key), could use ``contact'' directly
func (h *ShadowService) inform(pid peer.ID) error {
    return nil
}

// TODO: called after received an ``inform'' message
func (h *ShadowService) handleInform(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
    //TODO: if the node have the same public key with mine?

    //TODO: if true, set it as my shadow node
    return nil, nil
}

func (h *ShadowService) RegisterShadow() error {
	return nil
}

func (h *ShadowService) PushStreamMeta(meta *pb.StreamMeta) error {
	return nil
}

func (h *ShadowService) handleStreamMeta(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	return nil, nil
}

// HandleStream is called by the underlying service handler method
func (h *ShadowService) HandleStream(env *pb.Envelope, pid peer.ID) (chan *pb.Envelope, chan error, chan interface{}) {
	return make(chan *pb.Envelope), make(chan error), make(chan interface{})
}

// Ping pings another peer
func (h *ShadowService) Ping(pid peer.ID) (service.PeerStatus, error) {
	return h.service.Ping(pid.Pretty())
}
