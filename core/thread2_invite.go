package core

import (
	"fmt"

	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/thread"
	thread2 "github.com/textileio/go-threads/core/thread"
)

// Invite peer to a thread
func (t *Textile) Invite(threadID string, peerID string) error {
	info, err := t.GetDBInfo(threadID)
	if err != nil {
		fmt.Println("error when get dbinfo,", err)
		return err
	}
	if !info.Key.Defined() {
		fmt.Println("got undefined db key")
	}
	if len(info.Addrs) == 0 {
		fmt.Println("got empty addresses")
	}

	// check type and number of group , return error when type == singleChat && number == 2
	gType, err := t.GroupInfoType(threadID)
	if err != nil{
		return err
	}
	gNumber, err := t.GroupInfoNumber(threadID)
	if err != nil{
		return err
	}
	if  gType == singleChat && gNumber == 2{
		fmt.Println("error when add peer to a single chat, it's full")
		return nil
	}

	// create invite envelope
	reg := &pb.Thread2Invite{
		ThreadId: threadID,
		PeerId:   t.node.PeerHost.ID().Pretty(),
		DbAddr:   info.Addrs[0].String(),
		DbKey:    info.Key.String(),
		//CollectionConfig: 把collectioninfo放进来
	}
	env, err := t.mail.NewEnvelope(pb.Message_THREAD2_INVITE, reg, nil, false)
	if err != nil {
		fmt.Println("error when new envelop,", err)
		return err
	}

	// send the envelope to peer through t.mail.SendMessage(peerID, envelope)
	err = t.mail.SendMessage(t.ctx, peerID, env)
	if err != nil {
		fmt.Println("error when send message", err)
		return err
	}
	return nil
}

// Handle invite, called by watchMailbox
//accept invite means creating a local thread, and listen to the update
func (t *Textile) handleInvite(env *pb.Envelope) error {
	// join the thread
	inform := &pb.Thread2Invite{}
	err := ptypes.UnmarshalAny(env.Message.Payload, inform)
	if err != nil {
		//log.Error(err);
		fmt.Println("error when unmarshal, ", err)
		return err
	}
	//get db info

	dbkey, err := thread.KeyFromString(inform.DbKey)
	if err != nil {
		fmt.Println("error when keyfromstring, ", err)
		return err
	}
	dbAddr, err := ma.NewMultiaddr(inform.DbAddr)
	if err != nil {
		fmt.Println("error when NewMultiaddr, ", err)
		return err
	}
	//newdbfromaddr
	// 用collectioninfo直接创建DB
	err = t.threadclient.NewDBFromAddr(t.ctx, dbAddr, dbkey)
	if err != nil {
		fmt.Println("failed to create new db from address")
		return err
	}

	threadId, err := thread2.Decode(inform.ThreadId)
	if err != nil {
		fmt.Println("error when decode string to threadid")
		return err
	}
	err = t.NewMembersCollection(threadId)
	if err != nil {
		fmt.Println("error when new member collection")
		return err
	}
	err = t.NewMessagesCollection(threadId)
	if err != nil {
		fmt.Println("error when new message collection")
		return err
	}
	err = t.NewGroupInfoCollection(threadId)
	if err != nil {
		fmt.Println("error when new message collection")
		return err
	}

	//Start listening new created thread
	err = t.ListenOneThread2(inform.ThreadId)
	if err != nil {
		log.Errorf("error when listen one thread2", err)
	}

	//check type and number of group , return error when type == singleChat && number == 2
	gType, err := t.GroupInfoType(inform.ThreadId)
	if err != nil{
		return err
	}
	gNumber, err := t.GroupInfoNumber(inform.ThreadId)
	if err != nil{
		return err
	}
	if  gType == singleChat && gNumber == 2{
		// stop listen to the threadDB or delete the thread new created***********************
		err := t.DeleteDB(inform.ThreadId)
		if err != nil {
			return err
		}
		fmt.Println("error when join to  a single chat, it's full")
		return nil
	}

	//add myself info to the thread collection of member
	_, err = t.CreateMemInstance(threadId, client.Instances{
		ThreadMember{MemberId: t.Account().Address(), Name: t.Name(), Role: member}})
	if err != nil {
		fmt.Println("Error when add myself info to the thread, ", err)
		return err
	}
	//modify the number of group member
	err = t.ModifyGroupNumber(inform.ThreadId)
	if err != nil {
		return err
	}
	return nil
}
