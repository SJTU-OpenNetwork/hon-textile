package mobile

import (
	"encoding/json"
)

func (m *Mobile) CreateGroup(name string) (string, error) {
	threadid, err := m.node.CreateGroup(name)
	if err != nil {
		return "", err
	}
	threadIdStr := threadid.String()
	return threadIdStr, nil
}

func (m *Mobile) CreateDB() (string, error) {
	threadid, err := m.node.CreateDB()
	if err != nil {
		return "", err
	}
	threadIdStr := threadid.String()
	return threadIdStr, nil
}

func (m *Mobile) ListDBs() ([]byte, error) {
	dblist, err := m.node.ListDBs()
	if err != nil {
		return nil, err
	}
	bytes, err := json.Marshal(dblist)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (m *Mobile) Thread2AddMessage(threadId string, mes string) error {
	return m.node.ThreadAddMessage(threadId, mes)
}

func (m *Mobile) Thread2InvitePeer(threadId string, peerid string) error {
	return m.node.Invite(threadId, peerid)
}

func (m *Mobile) Thread2DeleteMessage(threadId string, index string) error {
	return m.node.DeleteInstance(threadId, "GroupMessage", []string{index})
}

func (m *Mobile) Thread2DeleteMember(threadId string, index string) error {
	return m.node.DeleteInstance(threadId, "GroupMember", []string{index})
}

func (m *Mobile) ThreadAddPeer(threadID string, peer []byte) error {
	return nil
}

func (m *Mobile) TheadUpdatePeer(threadID string, peer []byte) error {
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
//func (m *Mobile) Thread2_AddBytes(threadid string, bytes []byte) error {
//	threadId, err := thread2.Decode(threadid)
//	if err != nil {
//		fmt.Println("Error when thread2 decode string to thread.ID")
//		return err
//	}
//	err = m.node.Thread2AddBytes(threadId, bytes)
//	if err != nil {
//		return err
//	}
//	return nil
//}
