package core

import (
	"bufio"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/libp2p/go-libp2p-core/crypto"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/db"
	threadutil "github.com/textileio/go-threads/util"
	"io/ioutil"
	"os"
	"time"
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
			"creator": {
				"type": "string",
				"description": "The group's creator."
			},
			"time": {
				"type": "integer",
				"description": "The group's created time."
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

	//message type
	textMessage = "TEXT_MESSAGE_THREAD2"
	pictureMessage = "PICTURE_MESSAGE_THREAD2"
	fileMessage = "FILE_MESSAGE_THREAD2"
	directoryMessage = "DIRECTORY_MESSAGE_THREAD2"
	ticketVideoMessage = "TICKET_VIDEO_MESSAGE_THREAD2"
	streamVideoMessage = "STREAM_VIDEO_MESSAGE_THREAD2"
	ticketVideoChunks = "TICKET_VIDEO_CHUNKS_MESSAGE_THREAD2"

)

type FileMessage struct {
	XMLName xml.Name `xml:"file_message"`
	Name    string   `xml:"name"`
	Path    string   `xml:"path"`
	Type    string   `xml:"type"`
	Size    int64   `xml:"size"`
	MesString string `xml:"mes_string"`
	VideoId string   `xml:"video_id"`
}
type ThreadMember struct {
	ID       string `json:"_id"`
	MemberId string `json:"member_id"`
	Name     string `json:"name"`
	Role     string `json:"role"`
}

type ThreadMessage struct {
	ID      string `json:"_id"`
	Sender  string `json:"sender"`
	Time    int 	`json:"time"`
	Type    string `json:"type"`
	Content string `json:"content"`
}

type ThreadGroup struct {
	ID      string `json:"_id"`
	Name    string `json:"name"`
	Creator string `json:"creator"`
	Time	int `json:"time"`
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
	//issuer,err := t.account.LibP2PPrivKey()
	//if err != nil{
	//	fmt.Println("error when t.account.LibP2PPrivKey()")
	//	return "",err
	//}
	//claims := jwt.StandardClaims{
	//	Subject:  threadId.String(),
	//	Issuer:   thread.NewLibp2pIdentity(issuer).GetPublic().String(),
	//	IssuedAt: time.Now().Unix(),
	//}
	//str, err := jwt.NewWithClaims(jwted25519.SigningMethodEd25519i, claims).SignedString(issuer)
	////str1,err := jwt.New(jwted25519.SigningMethodEd25519i).SigningString()
	//if err != nil {
	//		fmt.Println("error when jwt.NewWithClaims")
	//		return "",err
	//	}
	//fmt.Println("new token for group is:<",str,">")
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
	//get db key and
	info, err := t.GetDBInfo(threadId.String())
	if err != nil {
		fmt.Println("error when get dbinfo,", err)
		return "",err
	}
	if !info.Key.Defined() {
		fmt.Println("got undefined db key")
		return "",err
	}
	if len(info.Addrs) == 0 {
		fmt.Println("got empty addresses")
	}
	dbAddr :=   info.Addrs[0].String()
	dbKey :=    info.Key.String()
	fmt.Println("dbAddr:<",dbAddr,">")
	fmt.Println("dbKey:<",dbKey,">")
	return threadId, nil
}

func (t *Textile) CreateGroupFromToken(threadIdStr string, dbAddr string, dbKey string)  error {
	threadId, err := thread.Decode(threadIdStr)
	if err != nil {
		return err
	}
	dbAddr1, err := ma.NewMultiaddr(dbAddr)
	if err != nil {
		fmt.Println("error when NewMultiaddr, ", err)
		return err
	}
	dbkey1, err := thread.KeyFromString(dbKey)
	if err != nil {
		fmt.Println("error when keyfromstring, ", err)
		return err
	}

	err = t.threadclient.NewDBFromAddr(t.ctx, dbAddr1,dbkey1)
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

func (t *Textile) CreateGroupFromToken1(threadname string)  (string, error) {
	privateKey, _, err :=crypto.GenerateEd25519Key(rand.Reader)
	if err!= nil {
		return "",err
	}
	myIdentity := thread.NewLibp2pIdentity(privateKey)
	threadToken, err := t.threadclient.GetToken(t.ctx, myIdentity)
	threadId := thread.NewIDV1(thread.Raw, 32)
	fmt.Println("threadToken is <",string(threadToken),">")
	fmt.Println("thread Id is <",threadId.String(),">")
	err = t.threadclient.NewDB(t.ctx, threadId,db.WithNewManagedToken(threadToken))
	if err != nil {
		return "",err
	}
	err = t.NewMembersCollection(threadId)
	if err != nil {
		return "",err
	}
	err = t.NewMessagesCollection(threadId)
	if err != nil {
		return "",err
	}
	err = t.NewGroupInfoCollection(threadId)
	if err != nil {
		return "",err
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
		return "",err
	}

	return threadId.String(),nil
}

func (t *Textile) CreateGroupFromToken2(threadIdStr string, token string)  error {
	threadId, err := thread.Decode(threadIdStr)
	if err != nil {
		return err
	}
	threadToken := thread.Token(token)
	err = t.threadclient.NewDB(t.ctx, threadId,db.WithNewManagedToken(threadToken))
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
	threadId, err := thread.Decode(threadIdStr)
	if err != nil {
		return nil, err
	}
	dbinfo, err := t.threadclient.GetDBInfo(t.ctx, threadId)
	if err!= nil {
		return nil,err
	}
	return dbinfo, nil
}

//func (t *Textile) GetDBAddrKey(threadIdStr string) (string,string,error){
//	info,err := t.GetDBInfo(threadIdStr)
//	if err!=nil{
//		return "","",err
//	}
//	if !info.Key.Defined() {
//		fmt.Println("got undefined db key")
//	}
//	if len(info.Addrs) == 0 {
//		fmt.Println("got empty addresses")
//	}
//	DbAddr := info.Addrs[0].String()
//	DbKey := info.Key.String()
//	return DbAddr,DbKey,nil
//}

//not used for now
func (t *Textile) DeleteDB(threadIdStr string) (error) {
	threadId, err := thread.Decode(threadIdStr)
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
		client.Instances{&ThreadGroup{Name:groupName,Number:1,Creator:t.Account().Address(),Time:int(time.Now().Unix()),Type:groupChat,Flag:"groupInfo"}})
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
	threadId, err := thread.Decode(id)
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
	threadId, err := thread.Decode(id)
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
	threadId, err := thread.Decode(id)
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
func (t *Textile) ModifyMessageInstance(threadIdStr string, instanceId string, newContent string) error {
	threadId, err := thread.Decode(threadIdStr)
	if err != nil {
		return err
	}
	//instanceId := ids[0]
	err = t.threadclient.Save(t.ctx, threadId, collectionMessage, client.Instances{ThreadMessage{ID: instanceId, Content: newContent}})
	if err != nil {
		return err
	}
	return nil
}


//return content
func (t *Textile) FindMessageByID(threadIdStr string, instanceID string) (string,error){
	threadId, err := thread.Decode(threadIdStr)
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
	threadId, err := thread.Decode(threadIdStr)
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
	threadId, err := thread.Decode(id)
	if err != nil {
		return "",err
	}

	fm := &FileMessage{
		Type:textMessage,
		MesString:mes,
	}

	//enc := xml.NewEncoder(os.Stdout)
	//enc.Indent("  ", "    ")
	//Marshal xml struct to bytes then to string.
	var contentStr string
	if bytes,err := xml.Marshal(fm); err != nil {
		fmt.Printf("error: %v\n", err)
	}else {
		contentStr = string(bytes)
	}


	Ids, err := t.CreateMesInstance(threadId, client.Instances{
		&ThreadMessage{Sender: t.Account().Address(), Time: int(time.Now().Unix()), Content: contentStr}})
	if err != nil {
		return "",err
	}
	fmt.Println("added message: '", mes, "' to thread: '", id, "'")
	return Ids[0],nil
}

func (t *Textile) Thread2AddPicture(path string, threadIdStr string) (string, error){
	threadId, err := thread.Decode(threadIdStr)
	if err != nil {
		return "",err
	}
	log.Debugf("AddSimpleFile(%s, %s)", path, threadId)

	// Open file and get reader for file
	fileInfo, err := os.Stat(path)
	if err != nil {
		log.Error(err)
		return "", err
	}
	if fileInfo.IsDir() {
		err = errors.New("SimpleFile does not support directory")
		log.Error(err)
		return "", err
	}

	fi, err := os.Open(path)
	if err != nil {
		log.Error(err)
		return "", err
	}
	defer func() {
		err := fi.Close()
		if err != nil {
			log.Error(err)
		}
	}()

	// Add file to ipfs
	r := bufio.NewReader(fi)
	fileCid, err := ipfs.AddData(t.node, r, true, false)
	// resolvedPath, err := api.Unixfs().Add(t.ctx, files.NewReaderFile(fi), options.Unixfs.HashOnly(false), options.Unixfs.Chunker("size-1048576"))
	if err != nil {
		log.Error(err)
		return "", err
	}

	fm := &FileMessage{
		Name: fileInfo.Name(),
		Path: fileCid.String(),
		Type:pictureMessage,
		Size: fileInfo.Size(),
		}

	//enc := xml.NewEncoder(os.Stdout)
	//enc.Indent("  ", "    ")
	//Marshal xml struct to bytes then to string.
	var contentStr string
	if bytes,err := xml.Marshal(fm); err != nil {
		fmt.Printf("error: %v\n", err)
	}else {
		contentStr = string(bytes)
	}

	//Add file to thread2
	Ids, err := t.CreateMesInstance(threadId, client.Instances{
		//&ThreadMessage{Sender: t.Account().Address(), Time: int(time.Now().Unix()),Type:pictureMessage, Content: contentStr}})
		&ThreadMessage{Sender: t.Account().Address(), Time: int(time.Now().Unix()), Content: contentStr}})
	if err != nil {
		return "",err
	}
	return Ids[0],nil
}

func (t *Textile) Thread2AddFile(path string, threadIdStr string) (string, error){
	threadId, err := thread.Decode(threadIdStr)
	if err != nil {
		return "",err
	}
	log.Debugf("AddSimpleFile(%s, %s)", path, threadId)

	// Open file and get reader for file
	fileInfo, err := os.Stat(path)
	if err != nil {
		log.Error(err)
		return "", err
	}
	if fileInfo.IsDir() {
		err = errors.New("SimpleFile does not support directory")
		log.Error(err)
		return "", err
	}

	fi, err := os.Open(path)
	if err != nil {
		log.Error(err)
		return "", err
	}
	defer func() {
		err := fi.Close()
		if err != nil {
			log.Error(err)
		}
	}()

	// Add file to ipfs
	r := bufio.NewReader(fi)
	fileCid, err := ipfs.AddData(t.node, r, true, false)
	// resolvedPath, err := api.Unixfs().Add(t.ctx, files.NewReaderFile(fi), options.Unixfs.HashOnly(false), options.Unixfs.Chunker("size-1048576"))
	if err != nil {
		log.Error(err)
		return "", err
	}

	fm := &FileMessage{
		Name: fileInfo.Name(),
		Path: fileCid.String(),
		Type:fileMessage,
		Size: fileInfo.Size(),
	}

	//enc := xml.NewEncoder(os.Stdout)
	//enc.Indent("  ", "    ")
	//Marshal xml struct to bytes then to string.
	var contentStr string
	if bytes,err := xml.Marshal(fm); err != nil {
		fmt.Printf("error: %v\n", err)
	}else {
		contentStr = string(bytes)
	}

	//Add file to thread2
	Ids, err := t.CreateMesInstance(threadId, client.Instances{
		//&ThreadMessage{Sender: t.Account().Address(), Time: int(time.Now().Unix()),Type:pictureMessage, Content: contentStr}})
		&ThreadMessage{Sender: t.Account().Address(), Time: int(time.Now().Unix()), Content: contentStr}})
	if err != nil {
		return "",err
	}
	return Ids[0],nil
}

func (t *Textile) Thread2AddDirectory(threadIdStr string, pth string) (string, error) {
	threadId, err := thread.Decode(threadIdStr)
	if err != nil {
		return "",err
	}
	dirPath,err := t.thread2AddDirectory(pth)
	if err != nil{
		return "",err
	}
	fileInfo, err := os.Stat(pth)
	if err != nil {
		log.Error(err)
		return "", err
	}
	fm := &FileMessage{
		Name: fileInfo.Name(),
		Type:directoryMessage,
		Size: fileInfo.Size(),
		MesString: dirPath,
	}

	//enc := xml.NewEncoder(os.Stdout)
	//enc.Indent("  ", "    ")
	//Marshal xml struct to bytes then to string.
	var contentStr string
	if bytes,err := xml.Marshal(fm); err != nil {
		fmt.Printf("error: %v\n", err)
	}else {
		contentStr = string(bytes)
	}

	//Add file to thread2
	Ids, err := t.CreateMesInstance(threadId, client.Instances{
		//&ThreadMessage{Sender: t.Account().Address(), Time: int(time.Now().Unix()),Type:pictureMessage, Content: contentStr}})
		&ThreadMessage{Sender: t.Account().Address(), Time: int(time.Now().Unix()), Content: contentStr}})
	if err != nil {
		return "",err
	}
	return Ids[0],nil
}

func (t *Textile) Thread2AddTicketVideo(threadIdStr string, videoId string) (string,error) {
	threadId, err := thread.Decode(threadIdStr)
	if err != nil {
		return "",err
	}

	//video := t.GetVideo(videoId)
	//if video == nil {
	//	return "",ErrVideoNotFound
	//}

//	log.Debugf("video caption: %s, Path: %s, VideoId: %s", video.Caption, video.Poster,video.Id)
	fm := &FileMessage{
		//Name: video.Caption,//video caption
		Path: videoId,//video poster hash
		Type:ticketVideoMessage,
		//VideoId: video.Id,
	}
	var contentStr string
	if bytes,err := xml.Marshal(fm); err != nil {
		fmt.Printf("error: %v\n", err)
	}else {
		contentStr = string(bytes)
	}

	//Add file to thread2
	Ids, err := t.CreateMesInstance(threadId, client.Instances{
		&ThreadMessage{Sender: t.Account().Address(), Time: int(time.Now().Unix()), Content: contentStr}})
	if err != nil {
		return "",err
	}
	return Ids[0],nil
}

func (t *Textile) Thread2UpdateVideoChunk(threadIdStr string, instanceId string, videoId string, tsArray string ) error {
	threadId, err := thread.Decode(threadIdStr)
	if err != nil {
		return err
	}

	//video := t.GetVideo(videoId)
	//if video == nil {
	//	return ErrVideoNotFound
	//}
	fm := &FileMessage{
		//Name: video.Caption,//video caption
		//Path: video.Poster,//video poster hash
		Type:ticketVideoChunks,
		VideoId: videoId,
		MesString:tsArray,
	}
	var contentStr string
	if bytes,err := xml.Marshal(fm); err != nil {
		fmt.Printf("error: %v\n", err)
	}else {
		contentStr = string(bytes)
	}

	//Save video chunk change to thread2
	err = t.threadclient.Save(t.ctx, threadId, collectionMessage, client.Instances{ThreadMessage{ID: instanceId, Content: contentStr}})
	if err != nil {
		return err
	}

	return nil
}

//Users can modify the message content.
func (t *Textile) GroupInfo(threadid string, instanceId string) (string,error) {
	threadId, err := thread.Decode(threadid)
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
	threadId, err := thread.Decode(threadIdStr)
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
	threadId, err := thread.Decode(threadIdStr)
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
	threadId, err := thread.Decode(threadIdStr)
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
	threadId, err := thread.Decode(threadIdStr)
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
	threadId, err := thread.Decode(threadIdStr)
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

func (t *Textile) IpfsAddDirectory(pth string,xml string) (string, error) {
	rd, err := ioutil.ReadDir(pth)
	for _, fi := range rd {
		if fi.IsDir() {
			xml = xml + "<dir>" + "<dirName>" + fi.Name() + "</dirName>"
			xml,err = t.IpfsAddDirectory(pth + "/" + fi.Name(),xml)
			if err != nil{
				return "",err
			}
			xml = xml + "</dir>"
		} else {
			// Open file and get reader for file
			// Add file to ipfs
			filePath := pth + "/" + fi.Name()
			f, err := os.Open(filePath)
			if err != nil{
				return "",err
			}
			r := bufio.NewReader(f)
			fileCid, err := ipfs.AddData(t.node, r, true, false)
			if err != nil {
				return "",err
			}
			xml = xml + "<file>" +
				"<fileName>" + fi.Name() + "</fileName>" +
				"<fileHash>" + fileCid.String() + "</fileHash>" +
						"</file>"
		}
	}
	return xml,nil
}

func (t *Textile) thread2AddDirectory(pth string) (string, error) {
	fileInfo, err := os.Stat(pth)
	if err != nil {
		log.Error(err)
		return "", err
	}
	if !fileInfo.IsDir() {
		err = errors.New("Thread2AddDirectory only supports adding directory to thread")
		log.Error(err)
		return "", err
	}
	//var build strings.Builder

	filein,err  := t.IpfsAddDirectory(pth,"")
	if err != nil{
		return "",err
	}
	res := "<dir>"+
			"<dirName>" + fileInfo.Name() +"</dirName>" +
			filein +
			"</dir>"
	return res,nil
}



