package core

import (
	"errors"
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
				"type": "string",
				"description": "The time of sending."
			},
			"content": {
				"type": "string",
				"description": "The content of a instance."

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
	Time    string `json:"time"`
	Content string `json:"content"`
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
func (t *Textile) CreateGroup() (thread.ID, error) {
	threadId := thread.NewIDV1(thread.Raw, 32)
	//actx, _ := context.WithTimeout(t.ctx, addTimeout)
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

	//Start listening new created thread
	t.ListenThread2s()
	//add myself info to the thread collection of member
	_, err = t.CreateInstance(threadId, collectionMember, client.Instances{
		&ThreadMember{MemberId: t.Account().Address(), Name: t.Name(), Role: owner}})
	if err != nil {
		fmt.Println("Error when add myself info to the thread")
		return threadId, err
	}


	//_, err = t.CreateInstance(threadId, collectionMessage, client.Instances{
	//	&ThreadMessage{Sender: t.Account().Address(), Time: time.Now().String(), Content: "123456789"}})
	//if err != nil {
	//	fmt.Println("Error when add myself info to the thread")
	//	return threadId, err
	//}
	return threadId, nil

}

func (t *Textile) CreateDB() (thread.ID, error) {
	id := thread.NewIDV1(thread.Raw, 32)

	//actx, _ := context.WithTimeout(t.ctx, addTimeout)
	//name1 := "db1"
	//err :=t.threadclient.NewDB(actx,id,db.WithNewManagedName(name1))
	err := t.threadclient.NewDB(t.ctx, id)

	//err :=t.threadclient.NewDB(t.ctx,id)

	if err != nil {
		return "", err
	}
	return id, nil
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
	err := t.threadclient.NewCollection(t.ctx, threadId, db.CollectionConfig{Name: collectionMember, Schema: threadutil.SchemaFromSchemaString(schemaMember)})
	if err != nil {
		return err
	}
	return nil
}

func (t *Textile) NewMessagesCollection(threadId thread.ID) error {
	err := t.threadclient.NewCollection(t.ctx, threadId, db.CollectionConfig{Name: collectionMessage, Schema: threadutil.SchemaFromSchemaString(schemaMessage)})
	if err != nil {
		return err
	}
	return nil
}

//Create instances objects.
func (t *Textile) CreateInstance(id thread.ID, ctype string, instances client.Instances) ([]string, error) {
	switch ctype {
	case collectionMember:
		instanceIds, err := t.threadclient.Create(t.ctx, id, collectionMember, instances)
		if err != nil {
			return nil, err
		}
		return instanceIds, nil
	case collectionMessage:
		instanceIds, err := t.threadclient.Create(t.ctx, id, collectionMessage, instances)
		fmt.Println("complete addString: ", instanceIds[0])
		if err != nil {
			return nil, err
		}
		return instanceIds, nil
	default:
		return nil, nil
	}
}

//Delete instance. Delete instance Through ID.
//Assume we get ids from CreateInstance, then we can use ids[0] to delete it.
func (t *Textile) DeleteInstance(id string, ctype string, instanceIDs []string) error {
	threadId, err := thread2.Decode(id)
	if err != nil {
		return err
	}
	switch ctype {
	case collectionMember:
		err := t.threadclient.Delete(t.ctx, threadId, collectionMember, instanceIDs)
		if err != nil {
			return err
		}
		return nil
	case collectionMessage:
		err := t.threadclient.Delete(t.ctx, threadId, collectionMessage, instanceIDs)
		if err != nil {
			return err
		}
		return nil
	default:
		return nil
	}
}

//Save used to modify instances, users use instanceId(ID) change specific instance,
//and users can modify the name and role of members.
//ids is gotten from creat instance.
func (t *Textile) SaveMemberInstance(id thread.ID, ids []string, name string, role string) error {
	instanceId := ids[0]
	err := t.threadclient.Save(t.ctx, id, collectionMember, client.Instances{ThreadMember{ID: instanceId, Name: name, Role: role}})
	if err != nil {
		return err
	}
	return nil
}

//Users can modify the message content.
func (t *Textile) SaveMessageInstance(id thread.ID, ids []string, newContent string) error {
	instanceId := ids[0]
	err := t.threadclient.Save(t.ctx, id, collectionMessage, client.Instances{ThreadMessage{ID: instanceId, Content: newContent}})
	if err != nil {
		return err
	}
	return nil
}

//add a string to the message collection of a thread
func (t *Textile) AddThreadDBString(id string, mes string) error {
	threadId, err := thread2.Decode(id)
	if err != nil {
		return err
	}
	_, err = t.CreateInstance(threadId, collectionMessage, client.Instances{
		&ThreadMessage{Sender: t.Account().Address(), Time: time.Now().String(), Content: mes}})
	if err != nil {
		return err
	}
	fmt.Println("added message: '", mes, "' to thread: '", id, "'")
	return nil
}

// ThreadClientSubscribe return a channel to listen the update of threads.
func (t *Textile) ThreadClientSubscribe(id thread.ID) (<-chan client.ListenEvent, error) {
	if t.threadclient == nil {
		fmt.Println("threadClient is nil")
		return nil, errors.New("threadClient is nil")
	}
	//Listen option is a filter in update, indicate which level update you want to listen , threadDB, collection or instance
	//?????? what will happen when opt is empty?
	opt := client.ListenOption{
		Collection: collectionMessage,
		//InstanceID: ,
	}
	return t.threadclient.Listen(t.ctx, id, []client.ListenOption{opt})
}

//func (t *Textile) AddNewPeer(thread)


//func unmarshalItems(items []interface{}) ([][]byte, error) {
//	values := make([][]byte, len(items))
//	for i, item := range items {
//		bytes, err := json.Marshal(item)
//		if err != nil {
//			return nil, err
//		}
//		values[i] = bytes
//	}
//	return values, nil
//}

