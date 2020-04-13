package api

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

// Printout stat of current shadow service.
func (a *Api) shadowStat(g *gin.Context) {
	views := a.Node.ShadowStat()
	pbJSON(g, http.StatusOK, )
}