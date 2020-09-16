package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	thread2 "github.com/textileio/go-threads/core/thread"
	"io/ioutil"
	"net/http"
)

func (a *Api) thread2ls(g *gin.Context) {
	threadSlice, err := a.Node.Thread2List()
	if err != nil {
		log.Error("Error when fetch the go-threads list: ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	} else {
		if threadSlice == nil {
			g.String(http.StatusOK, "")
		} else {
			g.JSON(http.StatusOK, threadSlice)
		}
	}
}

func (a *Api) thread2Create(g *gin.Context) {
	threadInfo, err := a.Node.Thread2CreateRaw()
	if err != nil {
		log.Error("Error when create thread2: ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	} else {
		g.JSON(http.StatusOK, threadInfo)
	}
}

func (a *Api) thread2AddString(g *gin.Context) {
	threadIdStr :=  g.Param("threadId")
	if threadIdStr == "" {
		g.String(http.StatusBadRequest, "threadId missed")
		return
	}
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		g.String(http.StatusBadRequest, "error when decode thread id: %s", err.Error())
		return
	}
	data, err := ioutil.ReadAll(g.Request.Body)
	if err != nil {
		g.String(http.StatusBadRequest, "error when read from request body: %s", err.Error())
		return
	}

	err = a.Node.Thread2AddBytes(threadId, "text",data)
	if err != nil {
		g.String(http.StatusBadGateway, "error when add bytes: %s", err.Error())
		return
	}

	g.Status(http.StatusOK)
}

func (a *Api) thread2AddFile(g *gin.Context)  {
	// Parse parameters
	// params are defined in cmd/thread2.go
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
	threadId, err := thread2.Decode(threadIdStr)
	if err != nil {
		g.String(http.StatusBadRequest, "error when decode thread id: %s", err.Error())
		return
	}
	filePath, ok := opts["filePath"]
	if !ok {
		g.String(http.StatusBadRequest, "missing file path")
		return
	}

	// Open File
	data, err := ioutil.ReadFile(filePath)
	//fileObj, err := os.Open(filePath)
	if err != nil {
		g.String(http.StatusBadRequest, err.Error())
	}

	// Add file to ipfs
	id,err := a.Node.AddData(data, false, false)
	if err != nil{
		fmt.Println("error :", err)
	}

	err = a.Node.Thread2AddBytes(threadId, "img", []byte(id))
	if err != nil {
		g.String(http.StatusBadRequest, err.Error())
		return
	}
}