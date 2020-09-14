package api

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (a *Api) thread2ls(g *gin.Context) {
	threadSlice, err := a.Node.Thread2List()
	if err != nil {
		log.Error("Error when fetch the go-threads list: ", err)
		g.String(http.StatusBadGateway, "Error: %v", err)
	} else {
		g.JSON(http.StatusOK, threadSlice)
	}
}
