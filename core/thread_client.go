package core

import (
	"fmt"
	"errors"
	"github.com/phayes/freeport"
	"github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/common"
	"github.com/textileio/go-threads/core/thread"
	thread2 "github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/db"
	"github.com/textileio/go-threads/util"
	"google.golang.org/grpc"
	"io/ioutil"
	"math/rand"
	"net"
	"time"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/textileio/go-threads/api"
	newthreadspb "github.com/textileio/go-threads/api/pb"
)


const (
	// Roles for member collection
	owner = "OWNER"
	admin = "ADMINISTRATOR"
	member = "GENERAL_MEMBER"

	collectionMember = "GroupMember"
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
				"type": "data-time",
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
	ID        string `json:"_id"`
	MemberId  string `json:"member_id"`
	Name 	  string `json:"name"`
	Role      string `json:"role"`
}

type ThreadMessage struct {
	ID        string `json:"_id"`
	Sender    string `json:"sender"`
	Time 	  time.Time `json:"time"`
	Content   string `json:"content"`
}


func makeServer() (ma.Multiaddr,error) {
	time.Sleep(time.Second * time.Duration(rand.Intn(5)))
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil,err
	}
	n, err := common.DefaultNetwork(dir, common.WithNetDebug(true), common.WithNetHostAddr(util.FreeLocalAddr()))
	if err != nil {
		return nil,err
	}
	n.Bootstrap(util.DefaultBoostrapPeers())
	service, err := api.NewService(n, api.Config{
		RepoPath: dir,
		Debug:    true,
	})
	if err != nil {
		return nil,err
	}
	port, err := freeport.GetFreePort()
	if err != nil {
		return nil,err
	}
	//our port default is 4001,so we dont need freeport.GetFreePort(), but it seems that thread port is different with ipfs.
	addr := util.MustParseAddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port))
	target, err := util.TCPAddrFromMultiAddr(addr)
	if err != nil {
		return nil,err
	}
	server := grpc.NewServer()
	listener, err := net.Listen("tcp", target)
	if err != nil {
		return nil,err
	}
	go func() {
		newthreadspb.RegisterAPIServer(server, service)
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve error: %v", err)
		}
	}()

	return addr,nil
}

//CreateGroup actually are two steps:
// create a threadDB and add two collections(member and message) to the DB.
func (t *Textile) CreateGroup() (thread.ID, error) {
	threadId := thread.NewIDV1(thread.Raw, 32)
	//actx, _ := context.WithTimeout(t.ctx, addTimeout)
	err := t.threadclient.NewDB(t.ctx,threadId)
	if err != nil{
		return "",err
	}

	err = t.NewMembersCollection(threadId)
	if err != nil{
		return "",err
	}
	err = t.NewMessagesCollection(threadId)
	if err != nil{
		return "",err
	}

	//Start listening new created thread
	t.ListenThread2s()
	//add myself info to the thread collection of member
	_,err = t.CreateInstance(threadId, collectionMember,client.Instances{
		ThreadMember{MemberId:t.Account().Address(), Name:t.Name(), Role: owner}})
	if err != nil{
		fmt.Println("Error when add myself info to the thread")
		return threadId,err
	}

	return threadId,nil
}

func (t *Textile) CreateDB() (thread.ID, error) {
	id := thread.NewIDV1(thread.Raw, 32)
	//actx, _ := context.WithTimeout(t.ctx, addTimeout)
	//name1 := "db1"
	//err :=t.threadclient.NewDB(actx,id,db.WithNewManagedName(name1))
	err :=t.threadclient.NewDB(t.ctx,id)
	if err != nil {
		return "",err
	}
	return id,nil
}


func (t *Textile) ListDBs() (map[thread.ID]*client.DBInfo, error) {
	return t.threadclient.ListDBs(t.ctx)
}

func (t *Textile) GetDBInfo(threadIdStr string) (*client.DBInfo,error) {
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return nil,err
	}
	dbinfo,err :=t.threadclient.GetDBInfo(t.ctx,threadId)
	return dbinfo,nil
}


//not used for now
func (t *Textile) DeleteDB(threadIdStr string) (*client.DBInfo,error) {
	return nil,nil
}


//create a new collection to a DB.
//And there are two types of collection in a DB: member and message,
// so we have two methods for collection creation
func (t *Textile) NewMembersCollection(threadId thread.ID) error {
	err := t.threadclient.NewCollection(t.ctx,threadId,db.CollectionConfig{Name: collectionMember,Schema: util.SchemaFromSchemaString(schemaMember)})
	if err!= nil {
		return err
	}
	return nil
}

func (t *Textile) NewMessagesCollection(threadId thread.ID) error {
	err := t.threadclient.NewCollection(t.ctx,threadId,db.CollectionConfig{Name: collectionMessage,Schema: util.SchemaFromSchemaString(schemaMessage)})
	if err!= nil {
		return err
	}
	return nil
}
/*
Instances is a list of collection instances.

CreateInstance actually create a new instance and add it to collection of thread.
SaveInstance actually is used to modify the instance we created and added before.
 */
//func (t *Textile) AddInstanceMember(id thread.ID, instances client.Instances) ([]string,error) {
//	ids, err := t.CreateInstance(id, collectionMember, instances)
//	if err != nil {
//		return nil,err
//	}
//
//
//	//err = t.SaveInstance(id, collectionMember, instances)
//	//if err != nil {
//	//	return nil,err
//	//}
//	return ids,nil
//}
//
//func (t *Textile) AddInstanceMessage(id thread.ID, instances client.Instances ) ([]string, error) {
//	ids, err := t.CreateInstance(id, collectionMessage, instances)
//	if err != nil {
//		return nil,err
//	}
//
//	err = t.SaveInstance(id, collectionMessage, instances)
//	if err != nil {
//		return nil,err
//	}
//	return ids,nil
//}

//Create instances objects.
func (t *Textile) CreateInstance(id thread.ID, ctype string, instances client.Instances) ([]string, error) {
	switch ctype {
	case collectionMember:
		instanceIds, err := t.threadclient.Create(t.ctx, id, collectionMember, instances)
		if err != nil {
			return nil,err
		}
		return instanceIds,nil
	case collectionMessage:
		instanceIds, err := t.threadclient.Create(t.ctx, id, collectionMessage, instances)
		if err != nil {
			return nil,err
		}
		return instanceIds,nil
	default:
		return nil,nil
	}
}

//Save used to modify instances, users use instanceId(ID) change specific instance,
//and users can modify the name and role of members.
//ids is gotten from creat instance.
func (t *Textile) SaveMemberInstance(id thread.ID, ids []string, name string, role string) error {
	instanceId := ids[0]
	err := t.threadclient.Save(t.ctx, id, collectionMember, client.Instances{ThreadMember{ID:instanceId,Name:name,Role:role}})
	if err != nil {
		return err
	}
	return nil
}
//Users can modify the message content.
func (t *Textile) SaveMessageInstance(id thread.ID, ids []string, newContent string) error {
	instanceId := ids[0]
	err := t.threadclient.Save(t.ctx, id, collectionMessage, client.Instances{ThreadMessage{ID:instanceId,Content:newContent}})
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
	_,err = t.CreateInstance(threadId, collectionMessage, client.Instances{ThreadMessage{Sender:t.Account().Address(),Time:time.Now(),Content:mes}})
	if err != nil {
		return err
	}
	fmt.Println("added message: '",mes,"' to thread: '", id,"'")
	return nil
}

// ThreadClientSubscribe return a channel to listen the update of threads.
func (t *Textile) ThreadClientSubscribe(id thread.ID) (<-chan client.ListenEvent, error){
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
	return t.threadclient.Listen(t.ctx, id,  []client.ListenOption{opt})
}