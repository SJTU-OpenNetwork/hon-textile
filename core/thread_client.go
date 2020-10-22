package core

import (
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/segmentio/ksuid"
	jwted25519 "github.com/textileio/go-threads/jwt"
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

	singleChat = "SINGLE_CHAT"
	groupChat = "GROUP_CHAT"

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
			},
			"number": {
				"type": "integer",
				"description": "Number of group member."
			},
			"flag": {
				"type": "string",
				"description": "Flag to find instance."
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
	Number	int	   `json:"number"`
	Flag	string `json:"flag"`
}

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
	_, err = t.CreateGroupInfoInstance(threadId, groupName)
	if err != nil {
		fmt.Println("Error when add group info")
		return threadId, err
	}

	return threadId, nil
}


func (t *Textile) CreateGroup2(groupName string) (thread.ID, error) {
	threadId := thread.NewIDV1(thread.Raw, 32)
	sk := t.node.PrivateKey
	issuer, err := peer.IDFromPrivateKey(sk)
	if err != nil {
		fmt.Println("error when peer.IDFromPrivateKey(sk)")
		return "", err
	}
	claims := &jwt.StandardClaims{
		Id:        ksuid.New().String(),
		IssuedAt:  time.Now().Unix(),
		Issuer:    issuer.Pretty(),
		Subject:   threadId.String(),
	}
	str, err := jwt.NewWithClaims(jwted25519.SigningMethodEd25519i, claims).SignedString(issuer)
	if err != nil {
		fmt.Println("error when jwt.NewWithClaims")
		return "",err
	}
	//return Token(str), nil
	//token, err := thread.NewToken(t.account.LibP2PPrivKey(),t.account.LibP2PPubKey())
	//
	//if err!=nil{
	//	return "",err
	//}
	//if token==""{
	//	fmt.Println("have not get token, it's nil")
	//}
	fmt.Println("new token for group is: ",thread.Token(str))
	err = t.threadclient.NewDB(t.ctx, threadId,db.WithNewManagedToken(thread.Token(str)))
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
	_, err = t.CreateGroupInfoInstance(threadId, groupName)
	if err != nil {
		fmt.Println("Error when add group info")
		return threadId, err
	}

	return threadId, nil
}

func (t *Textile) CreateGroupFromToken(threadIdStr string, tokenStr string)  error {
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return err
	}
	err = t.threadclient.NewDB(t.ctx, threadId,db.WithNewManagedToken(thread.Token(tokenStr)))
	if err != nil {
		return err
	}
	err = t.NewMembersCollection(threadId)
	if err != nil {
		return err
	}
	err = t.NewMessagesCollection(threadId)
	if err != nil {
		return err
	}
	err = t.NewGroupInfoCollection(threadId)
	if err != nil {
		return err
	}

	//Start listening new created thread
	err = t.ListenOneThread2(threadId.String())
	if err != nil {
		fmt.Println("Error when listen new group")
		return err
	}
	//add myself info to the thread collection of member
	_, err = t.CreateMemInstance(threadId,  client.Instances{
		ThreadMember{MemberId: t.Account().Address(), Name: t.Name(), Role: owner}})
	if err != nil {
		fmt.Println("Error when add myself info to the thread")
		return err
	}
	//add group info
	//_, err = t.CreateGroupInfoInstance(threadId, groupName)
	//if err != nil {
	//	fmt.Println("Error when add group info")
	//	return threadId, err
	//}

	return nil
}


//CreateChat is different with CreateGroup, one is used for one to one chatting,
//and another is used for group chatting.
func (t *Textile) CreateSingleChat(groupName string) (thread.ID, error) {
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
	_, err = t.CreateGroupInfoInstance2(threadId, groupName)
	if err != nil {
		fmt.Println("Error when add single chat info")
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
func (t *Textile) DeleteDB(threadIdStr string) (error) {
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return err
	}
	err = t.threadclient.DeleteDB(t.ctx,threadId)
	if err != nil {
		return err
	}
	return nil
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

//create a collection to storage group chat info, generally it has only one instance.
func (t *Textile) CreateGroupInfoInstance(id thread.ID, groupName string) ([]string, error) {
	instanceIds, err := t.threadclient.Create(t.ctx, id, collectionGroup,
		client.Instances{&ThreadGroup{Name:groupName,Number:1,Type:groupChat,Flag:"groupInfo"}})
	if err != nil {
		return nil, err
	}
	return instanceIds, nil
}

//create a collection to storage single chat info, indicate the type of it.
func (t *Textile) CreateGroupInfoInstance2(id thread.ID, groupName string) ([]string, error) {
	instanceIds, err := t.threadclient.Create(t.ctx, id, collectionGroup,
		client.Instances{&ThreadGroup{Name:groupName,Number:1,Type:singleChat,Flag:"groupInfo"}})
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
		fmt.Println("error when delete member in thread, ",err)
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
		fmt.Println("error when delete message in thread, ",err)
		return err
	}
	return nil
}

//Save used to modify instances, users use instanceId(ID) change specific instance,
//and users can modify the name and role of members.
//ids is gotten from creat instance.
func (t *Textile) ModifyMemberInstance(id string, instanceId string,  role string) (string,error) {
	//check the correction of role
	if role!=owner && role != admin && role != member {
		fmt.Println("error, the role given from cmd is not standard, only owner, admin and member can be used as role")
		return "",nil
	}
	threadId, err := thread2.Decode(id)
	if err != nil {
		return "",err
	}
	//instanceId := ids[0]
	err = t.threadclient.Save(t.ctx, threadId, collectionMember, client.Instances{ThreadMember{ID: instanceId, Role: role}})
	if err != nil {
		return "",err
	}
	return role,nil
}

//Users can modify the message content.
func (t *Textile) ModifyMessageInstance(id string, ids []string, newContent string) error {
	threadId, err := thread2.Decode(id)
	if err != nil {
		return err
	}
	instanceId := ids[0]
	err = t.threadclient.Save(t.ctx, threadId, collectionMessage, client.Instances{ThreadMessage{ID: instanceId, Content: newContent}})
	if err != nil {
		return err
	}
	return nil
}


//return content
func (t *Textile) FindMessageByID(threadIdStr string, instanceID string) (string,error){
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return "",err
	}
	exists, err := t.threadclient.Has(t.ctx, threadId, collectionMessage , []string{instanceID})
	if err != nil {
		fmt.Println("error when chenck whether thread has a instance,", err)
		return "",err
	}
	if !exists {
		fmt.Println("This thread hasn't instance you checked", err)
		return "",nil
	}

	newMessage := &ThreadMessage{}
	err = t.threadclient.FindByID(t.ctx, threadId, collectionMessage, instanceID, newMessage)
	if err != nil {
		fmt.Println("failed to find collection by id, ", err)
		return "",err
	}

	return newMessage.Content,nil
}

//return member role according to instanceID
func (t *Textile) FindMemberByID(threadIdStr string, instanceID string) (string,error) {
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return "",err
	}
	exists, err := t.threadclient.Has(t.ctx, threadId, collectionMember , []string{instanceID})
	if err != nil {
		fmt.Println("error when chenck whether thread has a instance,", err)
		return "",err
	}
	if !exists {
		fmt.Println("This thread hasn't instance you checked", err)
		return "",nil
	}

	checkedMember := &ThreadMember{}
	err = t.threadclient.FindByID(t.ctx, threadId, collectionMember, instanceID, checkedMember)
	if err != nil {
		fmt.Println("failed to find collection by id, ", err)
		return "",err
	}

	return checkedMember.Role,nil

}


//add a string to the message collection of a thread
func (t *Textile) Thread2AddMessage(id string, mes string) (string,error) {
	threadId, err := thread2.Decode(id)
	if err != nil {
		return "",err
	}
	Ids, err := t.CreateMesInstance(threadId, client.Instances{
		&ThreadMessage{Sender: t.Account().Address(), Time: int(time.Now().Unix()), Content: mes}})
	if err != nil {
		return "",err
	}
	fmt.Println("added message: '", mes, "' to thread: '", id, "'")
	return Ids[0],nil
}


//Users can modify the message content.
func (t *Textile) GroupInfo(threadid string, instanceId string) (string,error) {
	threadId, err := thread2.Decode(threadid)
	if err != nil {
		return "",err
	}
	//
	//q := db.Where("").Eq("groupInfo")
	//rawResults, err := t.threadclient.Find(t.ctx, threadId, collectionGroup, q, &ThreadGroup{})
	//if err != nil {
	//	fmt.Println("failed to find ", err)
	//}
	//results := rawResults.([]*ThreadGroup)
	//if len(results) != 1 {
	//	fmt.Println("expected 1 result, but got ", len(results))
	//}
	//groupName := results[0].Name
	groupInfo := &ThreadGroup{}
	err = t.threadclient.FindByID(t.ctx, threadId, collectionGroup, instanceId, groupInfo)
	if err != nil {
		fmt.Println("failed to find collection by id: ", err)
	}
	return groupInfo.Name, nil
}

func (t *Textile) GroupInfoName(threadIdStr string) (string, error) {
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return "",err
	}
	q := db.Where("flag").Eq("groupInfo")
	rawResults, err := t.threadclient.Find(t.ctx, threadId, collectionGroup, q, &ThreadGroup{})
	if err != nil {
		fmt.Println("failed to find: ", err)
		return "",err
	}
	results := rawResults.([]*ThreadGroup)
	if len(results) != 1 {
		fmt.Println("expected 1 result, but got ", len(results))
	}
	name := results[0].Name
	return name, nil
}

func (t *Textile) GroupInfoType(threadIdStr string) (string, error) {
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return "",err
	}
	q := db.Where("flag").Eq("groupInfo")
	rawResults, err := t.threadclient.Find(t.ctx, threadId, collectionGroup, q, &ThreadGroup{})
	if err != nil {
		fmt.Println("failed to find: ", err)
		return "",err
	}
	results := rawResults.([]*ThreadGroup)
	if len(results) != 1 {
		fmt.Println("expected 1 result, but got ", len(results))
	}
	gType := results[0].Type
	return gType, nil
}

//Return the member number in a threadDB.
func (t *Textile) GroupInfoNumber(threadIdStr string) (int, error) {
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return 0,err
	}
	q := db.Where("flag").Eq("groupInfo")
	rawResults, err := t.threadclient.Find(t.ctx, threadId, collectionGroup, q, &ThreadGroup{})
	if err != nil {
		fmt.Println("failed to find: ", err)
		return 0,err
	}
	results := rawResults.([]*ThreadGroup)
	if len(results) != 1 {
		fmt.Println("expected 1 result, but got ", len(results))
	}
	memNumber := results[0].Number
	return memNumber, nil
}

//Users can modify the message content.
func (t *Textile) ModifyGroupName(threadIdStr string, newName string) error {
	// maybe we need add access control, only owner or admin can modify
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return err
	}
	q := db.Where("flag").Eq("groupInfo")
	rawResults, err := t.threadclient.Find(t.ctx, threadId, collectionGroup, q, &ThreadGroup{})
	if err != nil {
		fmt.Println("failed to find: ", err)
		return err
	}
	results := rawResults.([]*ThreadGroup)
	if len(results) != 1 {
		fmt.Println("expected 1 result, but got ", len(results))
	}
	groupInfo := results[0]
	groupInfo.Name = newName
	err = t.threadclient.Save(t.ctx, threadId, collectionGroup, client.Instances{groupInfo})
	if err != nil {
		fmt.Println("failed to save a new name for group, ", err)
		return err
	}
	return nil
}

//Used when join to a threadDB
func (t *Textile) ModifyGroupNumber(threadIdStr string) error {
	// maybe we need add access control, only owner or admin can modify
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		return err
	}

	q := db.Where("flag").Eq("groupInfo")
	rawResults, err := t.threadclient.Find(t.ctx, threadId, collectionGroup, q, &ThreadGroup{})
	if err != nil {
		fmt.Println("failed to find: ", err)
		return err
	}
	results := rawResults.([]*ThreadGroup)
	if len(results) != 1 {
		fmt.Println("expected 1 result, but got ", len(results))
	}
	groupInfo := results[0]
	groupInfo.Number = groupInfo.Number + 1
	err = t.threadclient.Save(t.ctx, threadId, collectionGroup, client.Instances{groupInfo})
	if err != nil {
		fmt.Println("failed to save a new name for group, ", err)
		return err
	}
	return nil
}
