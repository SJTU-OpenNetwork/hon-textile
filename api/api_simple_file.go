package api

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (a *Api) addSimpleFile(g *gin.Context) {
	// Parse parameters
	// params are defined in cmd/stream.go
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	path, ok := opts["path"]
	if !ok {
		g.String(http.StatusBadRequest, "missing path")
		return
	}
	threadId, ok := opts["threadId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing threadId")
		return
	}

	block, err := a.Node.AddSimpleFile(path, threadId)
	pbJSON(g, http.StatusCreated, block)
}
