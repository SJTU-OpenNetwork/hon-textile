package api

import (
	"github.com/gin-gonic/gin"
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
	if err != nil {
		log.Error("Error when create thread client: ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	} else {
		g.JSON(http.StatusOK, threadId)
	}
}