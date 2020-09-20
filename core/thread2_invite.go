package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
	"github.com/libp2p/go-libp2p-core/peer"
)

// Invite peer to a thread
func (t *Textile) Invite(threadID string, peerID peer.ID) error {

	// create invite envelope
	reg := &pb.Thread_Invite{
		Thread: threadId,
		Node:   node,
		Sig:    sig,
		Block:  block,
	}
	//textile have not a service field, so i use threads.service
	env, err := t.mail.NewEnvelope(pb.Message_THREAD_INVITE, reg, nil, false)
	if err != nil {
		return err
	}
	// send the envelope to peer through t.mail.SendMessage(peerID, envelope)

	err = t.mail.SendMessage(t.ctx,peerID.Pretty(),env)
	if err != nil {
		return err
	}
	return nil
}

// Handle invite, called by watchMailbox
//accept invite means creating a local thread, and listen to the update
func (t *Textile) handleInvite(env *pb.Envelope) error {
	// join the thread
	inform := &pb.Thread_Invite{}
	err := ptypes.UnmarshalAny(env.Message.Payload, inform);
	if err != nil {
		//log.Error(err);
		return nil, err
	}
	return nil
}
