package api

import (
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
	threadId := thread2.ID(threadIdStr)
	data, err := ioutil.ReadAll(g.Request.Body)
	if err != nil {
		g.String(http.StatusBadRequest, "error when read from request body: %s", err.Error())
		return
	}

	err = a.Node.Thread2AddBytes(threadId, data)
	if err != nil {
		g.String(http.StatusBadGateway, "error when add bytes: %s", err.Error())
		return
	}

	g.Status(http.StatusOK)
}