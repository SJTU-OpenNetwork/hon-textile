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
		g.String(http.StatusBadGateway, "missing threadId")
		return
	}

	block, err := a.Node.AddSimpleFile(path, threadId)
	if err != nil {
		log.Error(err)
		g.String(http.StatusBadRequest, "error occur")
		return
	}
	log.Debugf("Api done add simple file\n%s", block.String())
	pbJSON(g, http.StatusOK, block)
	return
}
