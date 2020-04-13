package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
	"github.com/libp2p/go-libp2p-core/peer"
)

func (t *Textile) ShadowStat() *pb.ShadowStat {
	return t.shadow.ShadowStat()
}

// shadowMsgRecv is called by shadow service when receive a new stream meta.
func (t *Textile) shadowMsgRecv(env *pb.Envelope, pid peer.ID) error {
	log.Debug("shadowMsgRecv: Receive a stream meta")
	meta := new(pb.StreamMeta)
	err := ptypes.UnmarshalAny(env.Message.Payload, meta)
	if err != nil {
		return err
	}

	if env.Message.Request == 0 {
		last := t.datastore.StreamBlocks().LastIndex(meta.Id)
		config := &pb.StreamRequest {
			Id: meta.Id,
			StreamMap: 1,
			StartIndex: last,
		}
		log.Debugf("request stream data from the user, start index: %d", last)
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
			log.Errorf("request %s failed", pid.Pretty())
		} else {
			t.SubscribeNotify(config.Id, true)
		}
	} else {
		log.Debugf("request stream data from the network")
		t.SubscribeStream(meta.Id)
	}
	return nil
}

