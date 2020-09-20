package core

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
)

// Invite peer to a thread
func (t *Textile) Invite(threadID string, peerID string) error {

	// create invite envelope
	reg := &pb.Thread2Invite{
		ThreadId: threadID,
		PeerId:   t.node.PeerHost.ID().Pretty(),
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
	info, err := t.GetDBInfo(inform.ThreadId)
	if err != nil {
		return err
	}
	if !info.Key.Defined() {
		fmt.Println("got undefined db key")
	}
	if len(info.Addrs) == 0 {
		fmt.Println("got empty addresses")
	}
	//newdbfromaddr
	err = t.threadclient.NewDBFromAddr(t.ctx, info.Addrs[0], info.Key)
	if err!= nil {
		fmt.Println("failed to create new db from address")
		return err
	}
	return nil
}
