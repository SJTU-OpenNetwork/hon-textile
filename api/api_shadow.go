package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
)

// Printout stat of current shadow service.
func (a *Api) shadowStat(g *gin.Context) {
	views := a.Node.ShadowStat()
	pbJSON(g, http.StatusOK, views)
}

func (a *Api) shadowServePeer(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	pubkey, ok := opts["pubkey"]
	if !ok {
		g.String(http.StatusBadRequest, "missing streamId")
		return
	}
	fmt.Printf("Try to set serve peer %s.\n", pubkey)
	err = a.Node.SetServePeer(pubkey)
	if err != nil {
		g.String(http.StatusBadRequest, err.Error())
		return
	}

}