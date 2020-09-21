package core

import (
	"fmt"
	"time"

	"github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/thread"
	thread2 "github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/db"
	threadutil "github.com/textileio/go-threads/util"
)

const (
	// Roles for member collection
	owner  = "OWNER"
	admin  = "ADMINISTRATOR"
	member = "GENERAL_MEMBER"

	collectionMember  = "GroupMember"
	collectionMessage = "GroupMessage"
	collectionGroup = "GroupInfo"
	//In go-threads schema, properties must have _id to indicate the instance's id.
	schemaMember = `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "` + collectionMember + `",
		"type": "object",
		"properties": {
			"_id": {
				"type": "string",
				"description": "The instance's id."
			},
			"memberId": {
				"type": "string",
				"description": "The member's id."
			},
			"name": {
				"type": "string",
				"description": "The member's' name."
			},
			"role": {
				"type": "string",
				"description": "Role represent member's access."

			}
		}
	}`

	schemaMessage = `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "` + collectionMessage + `",
		"type": "object",
		"properties": {
			"_id": {
				"type": "string",
				"description": "The instance's id."
			},
			"sender": {
				"type": "string",
				"description": "The sender's id."
			},
			"time": {
				"type": "integer",
				"description": "The time of sending."
			},
			"content": {
				"type": "string",
				"description": "The content of a instance."

			}
		}
	}`

	schemaGroup = `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "` + collectionGroup + `",
		"type": "object",
		"properties": {
			"_id": {
				"type": "string",
				"description": "The instance's id."
			},
			"name": {
				"type": "string",
				"description": "The group's id."
			},
			"type": {
				"type": "string",
				"description": "The group's type."
			}
		}
	}`

)

type ThreadMember struct {
	ID       string `json:"_id"`
	MemberId string `json:"member_id"`
	Name     string `json:"name"`
	Role     string `json:"role"`
}

type ThreadMessage struct {
	ID      string `json:"_id"`
	Sender  string `json:"sender"`
	Time    int `json:"time"`
	Content string `json:"content"`
}

type ThreadGroup struct {
	ID      string `json:"_id"`
	Name    string `json:"name"`
	Type    string `json:"type"`
}

//func (t *Textile) makeServer() (ma.Multiaddr, error) {
//	//time.Sleep(time.Second * time.Duration(rand.Intn(5)))
//	dir := path.Join(t.repoPath, "threads")
//	if !util.DirectoryExist(dir) {
//		if err := os.Mkdir(dir, os.ModePerm); err != nil {
//			log.Error("Error when create path for go-threads: ", err)
//			return nil, err
//		}
//	}
//
//	// TODO: check whether t.node.PeerHost.Addrs is corrert!!!
//
//	n, err := common.DefaultNetwork(dir, common.WithNetDebug(true), common.WithNetHostAddr(t.node.PeerHost.Addrs[0]))
//	if err != nil {
//		return nil, err
//	}
//	n.Bootstrap(threadutil.DefaultBoostrapPeers())
//	service, err := api.NewService(n, api.Config{
//		RepoPath: dir,
//		Debug:    true,
//	})
//	if err != nil {
//		return nil, err
//	}
//	port, err := freeport.GetFreePort()
//	if err != nil {
//		return nil, err
//	}
//	//our port default is 4001,so we dont need freeport.GetFreePort(), but it seems that thread port is different with ipfs.
//	addr := threadutil.MustParseAddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port))
//	target, err := threadutil.TCPAddrFromMultiAddr(addr)
//	if err != nil {
//		return nil, err
//	}
//	server := grpc.NewServer()
//	listener, err := net.Listen("tcp", target)
//	if err != nil {
//		return nil, err
//	}
//	go func() {
//		newthreadspb.RegisterAPIServer(server, service)
//		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
//			log.Fatalf("serve error: %v", err)
//		}
//	}()
//
//	return addr, nil
//}

//CreateGroup actually are two steps:
// create a threadDB and add two collections(member and message) to the DB.
func (t *Textile) CreateGroup(groupName string) (thread.ID, error) {
	threadId := thread.NewIDV1(thread.Raw, 32)
	err := t.threadclient.NewDB(t.ctx, threadId)
	if err != nil {
		return "", err
	}
	err = t.NewMembersCollection(threadId)
	if err != nil {
		return "", err
	}
	err = t.NewMessagesCollection(threadId)
	if err != nil {
		return "", err
	}
	err = t.NewGroupInfoCollection(threadId)
	if err != nil {
		return "", err
	}

	//Start listening new created thread
	err = t.ListenOneThread2(threadId.String())
	if err != nil {
		fmt.Println("Error when listen new group")
		return "",err
	}
	//add myself info to the thread collection of member
	_, err = t.CreateMemInstance(threadId,  client.Instances{
		ThreadMember{MemberId: t.Account().Address(), Name: t.Name(), Role: owner}})
	if err != nil {
		fmt.Println("Error when add myself info to the thread")
		return threadId, err
	}
	//add group info
	_, err = t.CreateGroupInfo(threadId, groupName)
	if err != nil {
		fmt.Println("Error when add group info")
		return threadId, err
	}

	return threadId, nil
}

func (t *Textile) ListDBs() (map[thread.ID]*client.DBInfo, error) {
	return t.threadclient.ListDBs(t.ctx)
}

func (t *Textile) GetDBInfo(threadIdStr string) (*client.DBInfo, error) {
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return nil, err
	}
	dbinfo, err := t.threadclient.GetDBInfo(t.ctx, threadId)
	if err!= nil {
		return nil,err
	}
	return dbinfo, nil
}

//not used for now
func (t *Textile) DeleteDB(threadIdStr string) (*client.DBInfo, error) {
	return nil, nil
}

//create a new collection to a DB.
//And there are two types of collection in a DB: member and message,
// so we have two methods for collection creation
func (t *Textile) NewMembersCollection(threadId thread.ID) error {
	err := t.threadclient.NewCollection(t.ctx, threadId,
		db.CollectionConfig{Name: collectionMember, Schema: threadutil.SchemaFromSchemaString(schemaMember)})
	if err != nil {
		return err
	}
	return nil
}

func (t *Textile) NewMessagesCollection(threadId thread.ID) error {
	err := t.threadclient.NewCollection(t.ctx, threadId,
		db.CollectionConfig{Name: collectionMessage, Schema: threadutil.SchemaFromSchemaString(schemaMessage)})
	if err != nil {
		return err
	}
	return nil
}

func (t *Textile) NewGroupInfoCollection(threadId thread.ID) error {
	err := t.threadclient.NewCollection(t.ctx, threadId,
		db.CollectionConfig{Name: collectionGroup, Schema: threadutil.SchemaFromSchemaString(schemaGroup)})
	if err != nil {
		return err
	}
	return nil
}

//Create instances objects.
func (t *Textile) CreateMemInstance(id thread.ID, instances client.Instances) ([]string, error) {
		instanceIds, err := t.threadclient.Create(t.ctx, id, collectionMember, instances)
		if err != nil {
			return nil, err
		}
		return instanceIds, nil
}

func (t *Textile) CreateMesInstance(id thread.ID, instances client.Instances) ([]string, error) {
		instanceIds, err := t.threadclient.Create(t.ctx, id, collectionMessage, instances)
		//fmt.Println("complete addString: ", instanceIds[0])
		if err != nil {
			return nil, err
		}
		return instanceIds, nil
}

//create a collection to storage group info, generally it has only one instance.
func (t *Textile) CreateGroupInfo(id thread.ID, groupName string) ([]string, error) {
	instanceIds, err := t.threadclient.Create(t.ctx, id, collectionMessage,
		client.Instances{&ThreadGroup{Name:groupName}})
	if err != nil {
		return nil, err
	}
	return instanceIds, nil
}

//Delete instance. Delete instance Through ID.
//Assume we get ids from CreateInstance, then we can use ids[0] to delete it.
func (t *Textile) DeleteMemberInstance(id string, instanceIDs string) error {
	threadId, err := thread2.Decode(id)
	if err != nil {
		return err
	}
	err = t.threadclient.Delete(t.ctx, threadId, collectionMember, []string{instanceIDs})
	if err != nil {
		return err
	}
	return nil
}

func (t *Textile) DeleteMessageInstance(id string, instanceIDs string) error {
	threadId, err := thread2.Decode(id)
	if err != nil {
		return err
	}
	err = t.threadclient.Delete(t.ctx, threadId, collectionMessage, []string{instanceIDs})
	if err != nil {
		return err
	}
	return nil
}

//Save used to modify instances, users use instanceId(ID) change specific instance,
//and users can modify the name and role of members.
//ids is gotten from creat instance.
func (t *Textile) ModifyMemberInstance(id thread.ID, ids []string, name string, role string) error {
	instanceId := ids[0]
	err := t.threadclient.Save(t.ctx, id, collectionMember, client.Instances{ThreadMember{ID: instanceId, Name: name, Role: role}})
	if err != nil {
		return err
	}
	return nil
}

//Users can modify the message content.
func (t *Textile) ModifyMessageInstance(id thread.ID, ids []string, newContent string) error {
	instanceId := ids[0]
	err := t.threadclient.Save(t.ctx, id, collectionMessage, client.Instances{ThreadMessage{ID: instanceId, Content: newContent}})
	if err != nil {
		return err
	}
	return nil
}

func (t *Textile) FindMessageByID(threadIdStr string, instanceID string) (*ThreadMessage,error){
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return nil,err
	}
	exists, err := t.threadclient.Has(t.ctx, threadId, collectionMessage , []string{instanceID})
	if err != nil {
		fmt.Println("error when chenck whether thread has a instance,", err)
		return nil,err
	}
	if !exists {
		fmt.Println("This thread hasn't instance you checked", err)
		return nil,nil
	}

	newMessage := &ThreadMessage{}
	err = t.threadclient.FindByID(t.ctx, threadId, collectionMessage, instanceID, newMessage)
	if err != nil {
		fmt.Println("failed to find collection by id, ", err)
		return nil,err
	}

	return newMessage,nil
}

func (t *Textile) FindMemberByID(threadIdStr string, instanceID string) (*ThreadMember,error) {
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return nil,err
	}
	exists, err := t.threadclient.Has(t.ctx, threadId, collectionMember , []string{instanceID})
	if err != nil {
		fmt.Println("error when chenck whether thread has a instance,", err)
		return nil,err
	}
	if !exists {
		fmt.Println("This thread hasn't instance you checked", err)
		return nil,nil
	}

	checkedMember := &ThreadMember{}
	err = t.threadclient.FindByID(t.ctx, threadId, collectionMessage, instanceID, checkedMember)
	if err != nil {
		fmt.Println("failed to find collection by id, ", err)
		return nil,err
	}

	return checkedMember,nil

}


//add a string to the message collection of a thread
func (t *Textile) ThreadAddMessage(id string, mes string) error {
	threadId, err := thread2.Decode(id)
	if err != nil {
		return err
	}
	_, err = t.CreateMesInstance(threadId, client.Instances{
		&ThreadMessage{Sender: t.Account().Address(), Time: int(time.Now().Unix()), Content: mes}})
	if err != nil {
		return err
	}
	fmt.Println("added message: '", mes, "' to thread: '", id, "'")
	return nil
}


