// Service for sending/receving messages to the shadow node - add by Jerry 2020.04.05

package shadow

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/keypair"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	"github.com/golang/protobuf/ptypes"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	"sync"

	"github.com/SJTU-OpenNetwork/go-ipfs/core"
	//	"github.com/SJTU-OpenNetwork/hon-textile/repo/db"
	"github.com/SJTU-OpenNetwork/hon-textile/service"
	logging "github.com/ipfs/go-log"
)


// streamServiceProtocol is the current protocol tag
const shadowServiceProtocol = protocol.ID("/textile/shadow/1.0.0")
var log = logging.Logger("shadow")
var ErrWrongRole = fmt.Errorf("Wrong role.")	//shadow function called at normal peer or vice versa.

type ShadowService struct {
	service          *service.Service
	datastore        repo.Datastore
	online           bool
	msgRecv          func(*pb.Envelope, peer.ID) error
	isShadow		 bool
	address			 string  // public key. textile.account.Address()
    shadow           peer.ID //if isShadow == false, it maintains its shadow node
    //shadow 			 *shadowInfo
    users            []peer.ID //if isShadow == true, it maintains its user list
	lock             sync.Mutex
}

type shadowInfo struct {
	peerId		peer.ID
	multiAddress ma.Multiaddr
}

func NewShadowService(
	account *keypair.Full,
	node func() *core.IpfsNode,
	datastore repo.Datastore,
	msgRecv func(*pb.Envelope, peer.ID) error,
	isShadow bool,
	address string,
) *ShadowService {
	handler := &ShadowService{
		datastore:        datastore,
		msgRecv:          msgRecv,
		isShadow:		  isShadow,
		address:		  address,
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

func (h *ShadowService) GetShadow() peer.ID {
    return h.shadow
}

// Handle is called by the underlying service handler method
func (h *ShadowService) Handle(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	fmt.Printf("core/stream_service.go Handler: New message receive from %s.\n", pid.Pretty())
	switch env.Message.Type {
	case pb.Message_SHADOW_INFORM:
		return h.handleInform(env, pid)
	case pb.Message_SHADOW_STREAM_META:
		return h.handleStreamMeta(env, pid)
	case pb.Message_SHADOW_INFORM_RES:
		return h.handleInformRes(env, pid)
    default:
        return nil, nil
    }
}

// TODO: if the shadow node is disconnected, modify the work mode
func (h *ShadowService) PeerDisconnected(pid peer.ID) error{
    h.lock.Lock()
    defer h.lock.Unlock()

    if !h.isShadow {
        h.shadow = peer.ID("")

        // TODO: notify the upper layer (the textile core) that we lose our shadow node
    } else {
        h.removeUser(pid)
    }
	return nil
}

func (h *ShadowService) removeUser(pid peer.ID) {
    for id, value := range h.users {
        if value == pid {
            newUsers := append(h.users[:id], h.users[id+1:]...)
            h.users = newUsers
            return
        }
    }
}

func (h *ShadowService) addUser(pid peer.ID){
    h.users = append(h.users, pid)
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
	inform := &pb.ShadowInform{}
	inform.PublicKey = h.address
	env, err := h.service.NewEnvelope(pb.Message_SHADOW_INFORM, inform, nil, true); if err != nil {return err}
	err = h.service.SendMessage(nil, pid.Pretty(), env)

    return nil
}

// TODO: called after received an ``inform'' message
func (h *ShadowService) handleInform(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	if !h.isShadow {
		inform := &pb.ShadowInform{}
		err := ptypes.UnmarshalAny(env.Message.Payload, inform);
		if err != nil {
			//log.Error(err);
			return nil, err
		}
		//pk, err := pid.ExtractPublicKey()

		// Add it as shadow node if it has the same publickey
		res := &pb.ShadowInformResponse{}
		if inform.PublicKey == h.address {
			//h.shadow = pid
			h.RegisterShadow(pid)
			res.Accept = true
		} else {
			res.Accept = false
		}
		resenv, err := h.service.NewEnvelope(pb.Message_SHADOW_INFORM_RES, res, nil, false);
		if err != nil {
			return nil, err
		}

		return resenv, nil
	} else {
		return nil, ErrWrongRole
	}
}

func (h *ShadowService) handleInformRes(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error){
	if h.isShadow {
		res := &pb.ShadowInformResponse{}
		err := ptypes.UnmarshalAny(env.Message.Payload, res); if err != nil {return nil, err}
		if res.Accept {
			h.addUser(pid)
		}
		return nil, nil
	} else {
		return nil, ErrWrongRole
	}
}

func (h *ShadowService) RegisterShadow(id peer.ID) error {
	//h.lock.Lock()
	//defer h.lock.Unlock()
	h.shadow = id
	return nil
}

func (h *ShadowService) PushStreamMeta(meta *pb.StreamMeta) error {
    if h.shadow == peer.ID("") {
        return nil
    }

	env, err := h.service.NewEnvelope(pb.Message_SHADOW_STREAM_META, meta, nil, true); if err != nil {return err}
	return h.service.SendMessage(nil, h.shadow.Pretty(), env)
}

func (h *ShadowService) handleStreamMeta(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
    if h.isShadow {
        h.msgRecv(env, pid)
    }
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
