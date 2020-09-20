package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"net/http"
)

func (a *Api) threadClientAddGroup(g *gin.Context) {
	threadId,err := a.Node.CreateGroup()
	if err != nil {
		log.Error("Error when create thread client: ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	} else {
		g.JSON(http.StatusOK, threadId)
	}
}

func (a *Api) threadClientAddDB(g *gin.Context) {
	threadId,err := a.Node.CreateDB()
	fmt.Println("NewDB thread Id:",threadId)
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