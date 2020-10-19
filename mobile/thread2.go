package mobile

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/proto"
)

func (m *Mobile) CreateGroup(name string) (string, error) {
	threadid, err := m.node.CreateGroup(name)
	if err != nil {
		return "", err
	}
	threadIdStr := threadid.String()
	return threadIdStr, nil
}

func (m *Mobile) CreateSingleGroup(name string) (string, error) {
	threadid, err := m.node.CreateSingleChat(name)
	if err != nil {
		return "", err
	}
	threadIdStr := threadid.String()
	return threadIdStr, nil
}


func (m *Mobile) ListDBs() ([]byte, error) {
	views := &pb.Thread2List{
		Item: make([]*pb.Thread2, 0),
	}

	dbMap, err := m.node.ListDBs()
	if err != nil {
		return nil, err
	}
	//var threadList []string
	for k := range dbMap {
		threadId := k.String() //thread Id
		view := &pb.Thread2{ThreadId:threadId}
		views.Item = append(views.Item,view)
	}
	return proto.Marshal(views)
}

//return group name
func (m *Mobile) ThreadGroupName(threadId string) (string, error) {
	return m.node.GroupInfoName(threadId)
}

//return group name
func (m *Mobile) ThreadGroupType(threadId string) (string, error) {
	return m.node.GroupInfoType(threadId)
}

func (m *Mobile) ThreadModifyGroupInfo(threadId string, name string) error {
	return m.node.ModifyGroupName(threadId,  name)
}

//message add,remove and find
func (m *Mobile) Thread2AddMessage(threadId string, mes string) (string,error) {
	return m.node.Thread2AddMessage(threadId, mes)
}

func (m *Mobile) Thread2RemoveMessage(threadId string, instanceId string) error {
	return m.node.DeleteMessageInstance(threadId, instanceId)
}

func (m *Mobile) Thread2FindMessage(threadId string, instanceId string) (string, error) {
	return m.node.FindMessageByID(threadId, instanceId)

}

//peer invite remove find modify
func (m *Mobile) Thread2InviteMember(threadId string, peerid string) error {
	return m.node.Invite(threadId, peerid)
}

func (m *Mobile) Thread2DeleteMember(threadId string, instanceId string) error {
	return m.node.DeleteMemberInstance(threadId,instanceId)
}

func (m *Mobile) Thead2MemberRole(threadId string, instanceId string) (string, error) {
	return m.node.FindMemberByID(threadId, instanceId)
}

func (m *Mobile) Thead2MemberRoleChange(threadId string, instanceId string, role string) (string, error) {
	return m.node.ModifyMemberInstance(threadId, instanceId, role)
}

// invite peer and add message are using handler now.
type Thread2AddMesCallBack interface {
	Call(instanceId string, err error)
}

func (m *Mobile) Thread2AddStringMessage(threadId string, mes string, cb Thread2AddMesCallBack)  {
	m.node.WaitAdd(1, "Mobile.Thread2AddStringMessage")
	go func() {
		defer m.node.WaitDone("Mobile.Thread2AddStringMessage")
		cb.Call(m.node.Thread2AddMessage(threadId, mes))
	}()
}

type Thread2AddMemCallBack interface {
	Call(err error)
}

func (m *Mobile) Thread2AddMember(threadId string, peerId string, cb Thread2AddMemCallBack)  {
	m.node.WaitAdd(1, "Mobile.Thread2AddMember")
	go func() {
		defer m.node.WaitDone("Mobile.Thread2AddMember")
		cb.Call(m.node.Invite(threadId, peerId))
	}()
}


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
