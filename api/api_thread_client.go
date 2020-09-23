package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"net/http"
)

func (a *Api) threadClientAddGroup(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	groupName, ok := opts["groupName"]
	if !ok {
		g.String(http.StatusBadRequest, "missing groupName")
		return
	}
	threadId,err := a.Node.CreateGroup(groupName)
	if err != nil {
		log.Error("Error when create thread client: ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	} else {
		g.JSON(http.StatusOK, threadId)
	}
}


func (a *Api) threadClientListDB(g *gin.Context) {
	threadMap,err := a.Node.ListDBs()
	var threadList []string
	for k := range threadMap {
		threadList = append(threadList,k.String())
	}
	if err != nil {
		log.Error("Error when create thread client: ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	} else {
		g.JSON(http.StatusOK, threadList)
	}
}

func (a *Api) threadClientAddString(g *gin.Context) {
	threadIdStr :=  g.Param("threadId")
	if threadIdStr == "" {
		g.String(http.StatusBadRequest, "threadId missed")
		return
	}

	data, err := ioutil.ReadAll(g.Request.Body)
	if err != nil {
		g.String(http.StatusBadRequest, "error when read from request body: %s", err.Error())
		return
	}

	err = a.Node.ThreadAddMessage(threadIdStr,string(data[:]))
	if err != nil {
		return
	}
}

func (a *Api) threadClientRemoveMessage(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	threadIdStr, ok := opts["threadId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing threadId")
		return
	}
	instanceId, ok := opts["instanceId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing instance id")
		return
	}
	err  = a.Node.DeleteMessageInstance(threadIdStr,instanceId)
	if err != nil{
		return
	}

}

func (a *Api) threadClientFindMessage(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	threadIdStr, ok := opts["threadId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing threadId")
		return
	}
	instanceId, ok := opts["instanceId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing instance id")
		return
	}
	content,err  := a.Node.FindMessageByID(threadIdStr,instanceId)
	if err != nil {
		log.Error("Error when find message by id, ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	} else {
		g.JSON(http.StatusOK, content)
	}

}


func (a *Api) threadClientAddPeer(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	threadIdStr, ok := opts["threadId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing threadId")
		return
	}
	pid, ok := opts["peerId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing peer id")
		return
	}

	err = a.Node.Invite(threadIdStr,pid)
	if err!= nil {
		fmt.Println("error when invite peer")
	}
}

func (a *Api) threadClientRemovePeer(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	threadIdStr, ok := opts["threadId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing threadId")
		return
	}
	instanceId, ok := opts["instanceId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing instance id")
		return
	}
	err  = a.Node.DeleteMemberInstance(threadIdStr,instanceId)
	if err != nil{
		return
	}
}

func (a *Api) threadClientModPeer(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	threadIdStr, ok := opts["threadId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing threadId")
		return
	}
	instanceId, ok := opts["instanceId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing instance id")
		return
	}
	role, ok := opts["role"]
	if !ok {
		g.String(http.StatusBadRequest, "missing role")
		return
	}
	role,err = a.Node.ModifyMemberInstance(threadIdStr,instanceId,role)
	if err == nil && role != ""{
		g.JSON(http.StatusOK, role)
	} else {
		log.Error("Error when get member role, ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	}
}

func (a *Api) threadClientFindPeer(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	threadIdStr, ok := opts["threadId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing threadId")
		return
	}
	instanceId, ok := opts["instanceId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing instance id")
		return
	}
	role,err := a.Node.FindMemberByID(threadIdStr,instanceId)
	if err==nil && role != ""{
		g.JSON(http.StatusOK, role)
	}else if err == nil && role == ""{
		log.Error("Error when get member role, haven't the people you checked")
	}else {
		log.Error("Error when get member role,  ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	}
}

func (a *Api) threadClientGroupInfo(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	threadIdStr, ok := opts["threadId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing threadId")
		return
	}
	//instanceId, ok := opts["instanceId"]
	//if !ok {
	//	g.String(http.StatusBadRequest, "missing instanceId")
	//	return
	//}
	groupName,err := a.Node.GroupInfo2(threadIdStr)
	if err != nil {
		log.Error("Error when get group info, ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	} else {
		g.JSON(http.StatusOK, groupName)
	}
}

func (a *Api) threadClientNewGroupName(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	threadIdStr, ok := opts["threadId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing threadId")
		return
	}
	newName, ok := opts["name"]
	if !ok {
		g.String(http.StatusBadRequest, "missing new name")
		return
	}

	err = a.Node.ModifyGroupName(threadIdStr,newName)
	if err != nil {
		log.Error("Error when modify group info, ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	} else {
		g.JSON(http.StatusOK, newName)
	}
}