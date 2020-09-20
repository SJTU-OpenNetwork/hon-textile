package core

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
	"github.com/textileio/go-threads/core/thread"
	ma "github.com/multiformats/go-multiaddr"
)

// Invite peer to a thread
func (t *Textile) Invite(threadID string, peerID string) error {
	info, err := t.GetDBInfo(threadID)
	if err != nil {
		return err
	}
	if !info.Key.Defined() {
		fmt.Println("got undefined db key")
	}
	if len(info.Addrs) == 0 {
		fmt.Println("got empty addresses")
	}

	// create invite envelope
	reg := &pb.Thread2Invite{
		ThreadId: threadID,
		PeerId:   t.node.PeerHost.ID().Pretty(),
		DbAddr: info.Addrs[0].String(),
		DbKey: info.Key.String(),
	}
	env, err := t.mail.NewEnvelope(pb.Message_THREAD2_INVITE, reg, nil, false)
	if err != nil {
		return err
	}
	// send the envelope to peer through t.mail.SendMessage(peerID, envelope)
	err = t.mail.SendMessage(t.ctx, peerID, env)
	if err != nil {
		return err
	}
	return nil
}

// Handle invite, called by watchMailbox
//accept invite means creating a local thread, and listen to the update
func (t *Textile) handleInvite(env *pb.Envelope) error {
	// join the thread
	inform := &pb.Thread2Invite{}
	err := ptypes.UnmarshalAny(env.Message.Payload, inform);
	if err != nil {
		//log.Error(err);
		return err
	}
	//get db info

	dbkey,err := thread.KeyFromString(inform.DbKey)
	if err != nil {
		return err
	}
	dbAddr,err := ma.NewMultiaddr(inform.DbKey)
	if err != nil {
		return err
	}
	//newdbfromaddr
	err = t.threadclient.NewDBFromAddr(t.ctx, dbAddr , dbkey)
	if err!= nil {
		fmt.Println("failed to create new db from address")
		return err
	}
	return nil
}
