package core

import (
	"context"
	"github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/thread"
	thread2 "github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/db"
	"github.com/textileio/go-threads/util"
)


const (
	collectionName = "Group"

	schemamember = `{
		"$id": "https://example.com/person.schema.json",
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "` + collectionName + `",
		"type": "object",
		"properties": {
			"_id": {
				"type": "string",
				"description": "The instance's id."
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

	schemamessage = `{
		"$id": "https://example.com/person.schema.json",
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "` + collectionName + `",
		"type": "object",
		"properties": {
			"sender": {
				"type": "string",
				"description": "The sender's id."
			},
			"time": {
				"type": "data-time",
				"description": "The message's send time."
			},
			"content": {
				"type": "string"
				"description": "The content in thread.",
			}
		}
	}`
)
type Member struct {
	ID        string `json:"_id"`
	Name 	  string `json:"name,omitempty"`
	Role      string `json:"role,omitempty"`
}

type Message struct {
	Sender    string `json:"sender"`
	Time 	  string `json:"fullName,omitempty"`
	Content   string `json:"age,omitempty"`
}

//CreateGroup actually are two steps:
// create a threadDB and add two collections to the DB.
func (t *Textile) CreateGroup() (thread.ID, error) {
	threadid, err := t.CreateDB()
	if err != nil{
		return "",err
	}
	err = t.NewMembersCollection(threadid)
	if err != nil{
		return "",err
	}
	err = t.NewMessagesCollection(threadid)
	if err != nil{
		return "",err
	}
	return threadid,nil
}

func (t *Textile) CreateDB() (thread.ID, error) {
	id := thread.NewIDV1(thread.Raw, 32)
	actx, _ := context.WithTimeout(t.ctx, addTimeout)
	name1 := "db1"
	err :=t.threadclient.NewDB(actx,id,db.WithNewManagedName(name1))
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

func (t *Textile) DeleteDB(threadIdStr string) (*client.DBInfo,error) {
	return nil,nil
}

func (t *Textile) NewMembersCollection(threadId thread.ID) error {
	err := t.threadclient.NewCollection(t.ctx,threadId,db.CollectionConfig{Name: collectionName,Schema: util.SchemaFromSchemaString(schemamember)})
	if err!= nil {
		return err
	}
	return nil
}

func (t *Textile) NewMessagesCollection(threadId thread.ID) error {
	err := t.threadclient.NewCollection(t.ctx,threadId,db.CollectionConfig{Name: collectionName,Schema: util.SchemaFromSchemaString(schemamessage)})
	if err!= nil {
		return err
	}
	return nil
}