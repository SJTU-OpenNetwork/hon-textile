package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	//"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"net/http"
	"strconv"
)
func (a *Api)createStream(g *gin.Context) {
	// Parse parameters
	// params are defined in cmd/stream.go
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	streamId, ok := opts["streamId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing streamId")
		return
	}
	numSub, ok := opts["subNum"]
	numSubInt, err := strconv.Atoi(numSub)
	if !ok || err!=nil{
		g.String(http.StatusBadRequest, "missing streamId")
		return
	}

	//pb.StreamMeta{
	//	Id:                   "",
	//	Nsubstreams:          0,
	//}
	//a.Node.StartStream()
	fmt.Printf("Try to create stream %s with %d substreams.", streamId, numSubInt)
}
