package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/shadow"
	"github.com/golang/protobuf/ptypes"
	"github.com/libp2p/go-libp2p-core/peer"
)

func (t *Textile) ShadowStat() *pb.ShadowStat {
	//shadow.ConnectShadow("106.12.102.87",40101,t.node)
	return t.shadow.ShadowStat()
}

func (t* Textile) SetServePeer(pubkey string) error {
	if t.shadow.Shadow_serve_peer == "" {
		t.shadow.Shadow_serve_peer = pubkey
		log.Debugf("Set shadow serve peer as %s", pubkey)
	} else {
		log.Debugf("Change shadow serve peer from %s to %s", t.shadow.Shadow_serve_peer, pubkey)
		t.shadow.Shadow_serve_peer = pubkey
	}
	return nil
}

// Connect to shadow peer through tcp command directly.
func (t *Textile) ConnectShadowTCP(ip string, port int) error {
	return shadow.ConnectShadow(ip, port, t.node)
}

// shadowMsgRecv is called by shadow service when receive a new stream meta.
func (t *Textile) shadowMsgRecv(env *pb.Envelope, pid peer.ID) error {
	//log.Debug("shadowMsgRecv: Receive a stream meta")
	log.Debugf("shadowMsgRecv: Receive a stream meta from %s", pid.Pretty())
	meta := new(pb.StreamMeta)
	err := ptypes.UnmarshalAny(env.Message.Payload, meta)
	if err != nil {
		return err
	}
    err = t.datastore.StreamMetas().Add(meta);if err != nil {return err}
	if env.Message.Request == 1 {
		// Crreated by this account.
		last := t.datastore.StreamBlocks().LastIndex(meta.Id)
		config := &pb.StreamRequest {
			Id: meta.Id,
			StreamMap: 1,
			StartIndex: last,
		}
		log.Debugf("shadowMsgRecv: request stream data from the user, start index: %d", last)
		res, err := t.RequestStream(pid.Pretty(), config)
		if err != nil{
			log.Error(err)
			return err
		}
		response := new(pb.StreamRequestHandle)
		err = ptypes.UnmarshalAny(res.Message.Payload, response)
		if err!=nil {
			return err
		}
		if response.Value != 1 {
			log.Errorf("shadowMsgRecv: request %s failed", pid.Pretty())
		} else {
			t.SubscribeNotify(config.Id, true)
		}
	} else {
		log.Debugf("shadowMsgRecv: request stream data from the network")
		t.SubscribeStream(meta.Id)
	}
	return nil
}

