package core

import (
	"context"
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
type Member struct {
	ID        string `json:"_id"`
	MemberId  string `json:"member_id"`
	Name 	  string `json:"name"`
	Role      string `json:"role"`
}

type Message struct {
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
// create a threadDB and add two collections to the DB.
func (t *Textile) CreateGroup() (thread.ID, error) {
	threadId := thread.NewIDV1(thread.Raw, 32)
	actx, _ := context.WithTimeout(t.ctx, addTimeout)
	err := t.threadclient.NewDB(actx,threadId)
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
	return threadId,nil
}

func (t *Textile) CreateDB() (thread.ID, error) {
	id := thread.NewIDV1(thread.Raw, 32)
	actx, _ := context.WithTimeout(t.ctx, addTimeout)
	//name1 := "db1"
	//err :=t.threadclient.NewDB(actx,id,db.WithNewManagedName(name1))
	err :=t.threadclient.NewDB(actx,id)
	if err != nil {
		return "",err
	}
	return id,nil
}

//not used
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

// Instances is a list of collection instances.
//Because we have two type of collection, so we also need two
//methods to add instances to two kinds of collection.
func (t *Textile) AddInstanceMember(id thread.ID,  instances client.Instances) ([]string, error) {
	instanceIds, err := t.threadclient.Create(context.Background(), id, collectionMember, instances)
	if err != nil {
		return nil,err
	}
	return instanceIds,err
}

func (t *Textile) AddInstanceMessage(id thread.ID, instances client.Instances ) ([]string, error) {
	instanceIds, err := t.threadclient.Create(context.Background(), id, collectionMessage, instances)
	if err != nil {
		return nil,err
	}
	return instanceIds,err
}

//Create instances.
func (t *Textile) CreateInstanceMember(id thread.ID,  instances client.Instances) ([]string, error) {
	instanceIds, err := t.threadclient.Create(context.Background(), id, collectionMember, instances)
	if err != nil {
		return nil,err
	}
	return instanceIds,err
}

func (t *Textile) CreateInstanceMessage(id thread.ID, instances client.Instances ) ([]string, error) {
	instanceIds, err := t.threadclient.Create(context.Background(), id, collectionMessage, instances)
	if err != nil {
		return nil,err
	}
	return instanceIds,err
}

//Save instances to a collection(table) of threadDB
func (t *Textile) SaveInstanceMember(id thread.ID,  instances client.Instances) error {
	err := t.threadclient.Save(context.Background(), id, collectionMember, instances)
	if err != nil {
		return err
	}
	return nil
}

func (t *Textile) SaveInstanceMessage(id thread.ID, instances client.Instances) error {
	err := t.threadclient.Save(context.Background(), id, collectionMember, instances)
	if err != nil {
		return err
	}
	return nil
}