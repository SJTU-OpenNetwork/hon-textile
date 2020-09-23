package mobile



func (m *Mobile) CreateGroup(name string) (string, error) {
	threadid, err := m.node.CreateGroup(name)
	if err != nil {
		return "", err
	}
	threadIdStr := threadid.String()
	return threadIdStr, nil
}

func (m *Mobile) ListDBs() ([]string, error) {
	dbMap, err := m.node.ListDBs()
	if err != nil {
		return nil, err
	}
	var threadList []string
	for k := range dbMap {
		threadList = append(threadList,k.String())
	}
	return threadList, nil
}

//return group name
func (m *Mobile) ThreadGroupInfo(threadId string, instanceId string) (string, error) {
	return m.node.GroupInfo2(threadId)
}

func (m *Mobile) ThreadModifyGroupInfo(threadId string, name string) error {
	return m.node.ModifyGroupName(threadId,  name)
}

//message add,remove and find
func (m *Mobile) Thread2AddMessage(threadId string, mes string) error {
	return m.node.ThreadAddMessage(threadId, mes)
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
