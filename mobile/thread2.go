package mobile

import (
	"fmt"

	thread2 "github.com/textileio/go-threads/core/thread"
)

func (m *Mobile) Thread2AddMessage(threadID string, msg []byte) error {
	return m.node.ThreadAddMessage(threadID, string(msg))
}

func (m *Mobile) ThreadRemoveMessage(threadID string, msgID []byte) error {
	return nil
}

func (m *Mobile) ThreadAddPeer(threadID string, peer []byte) error {
	return nil
}

func (m *Mobile) TheadUpdatePeer(threadID string, peer []byte) error {
	return nil
}

func (m *Mobile) TheradRemovePeer(threadID string, peerID []byte) error {
	return nil
}

func (m *Mobile) ThreadUpdateGroupInfo(threadID string, name []byte, des []byte) error {
	return nil
}

/*
 Class xxxx implements Thread2Handler {
	@Override
	HandlerMsg() {
		xxxxxxx
	}
}
*/
type Thread2Handler interface {
	HandleMsg(threadId string, bytes []byte)
}

func Thread2Subscribe(handler Thread2Handler) {
	// core.Thread2Subscrtibe
	//Subscribe to thread2
	//go func() {
	//	var err error
	//	<- m.node.OnlineCh()
	//	thread2Ch, err := m.node.Thread2Subscribe()
	//	if err != nil {
	//		log.Error("Error when subscribe thread2: ", err)
	//		fmt.Println("Error when subscribe thread2: ", err)
	//		return
	//	}
	//	var threadRecord *core.Thread2Record
	//	for record := range thread2Ch {
	//		threadRecord, err = m.node.UnmarshalRecord(record)
	//		if err != nil {
	//			log.Error("Error when unmarshal record: ", err)
	//			continue
	//		}
	//
	//		//deal with the thread update
	//
	//		//msg = Green("Thread2 Record: "+"  "+threadRecord.ThreadId+" - "+threadRecord.LogId) + "\n" +
	//		//	Grey(string(threadRecord.Value))
	//		//fmt.Println(msg)
	//	}
	//}()

}

//In backend, we will not use pb.StreamMeta, we directly receive the []byte.
// [DEPRECATED]
func (m *Mobile) Thread2_AddBytes(threadid string, bytes []byte) error {
	threadId, err := thread2.Decode(threadid)
	if err != nil {
		fmt.Println("Error when thread2 decode string to thread.ID")
		return err
	}
	err = m.node.Thread2AddBytes(threadId, bytes)
	if err != nil {
		return err
	}
	return nil
}
