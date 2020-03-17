package api

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"net/http"
	"strconv"
)


func (a *Api)createStream(g *gin.Context) {
	defer fmt.Printf("api.createStream end success\n")
	// Parse parameters
	// params are defined in cmd/stream.go
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	threadId, ok := opts["threadId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing threadId")
		return
	}

	streamId, ok := opts["streamId"]
	if !ok {
		g.String(http.StatusBadRequest, "missing streamId")
		return
	}
	numSub, ok := opts["subNum"]
	numSubInt, _ := strconv.Atoi(numSub)
	if !ok || err!=nil{
		g.String(http.StatusBadRequest, "missing subNum")
		return
	}

	numSubInt32 := int32(numSubInt)
	//numSubInt, err := strconv.ParseInt(numSub, 10, 32)


	streamMeta := &pb.StreamMeta{
		Id:      streamId,
		Nsubstreams:    numSubInt32,
	}
	fmt.Printf("Try to create stream %s with %d substreams.\n", streamId, numSubInt)
	err = a.Node.StartStream(threadId, streamMeta)
	if err != nil {
		//fmt.Errorf(err.Error())
		g.String(http.StatusBadRequest, err.Error())
		return
	}
	g.String(http.StatusOK, "New stream create.")
	return
}

func (a *Api) streamAddFile(g *gin.Context) {
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
	filePath, ok := opts["filePath"]
	if !ok {
		g.String(http.StatusBadRequest, "missing streamId")
		return
	}

	// Open File
	bytes, err := ioutil.ReadFile(filePath)
	//fileObj, err := os.Open(filePath)
	if err != nil {
		g.String(http.StatusBadRequest, err.Error())
		return
	}

	streamFile := &pb.StreamFile{
		Data:                 bytes,
		Description:          nil,
	}
	// Call textile
	err = a.Node.StreamAddFile(streamId, streamFile)
	if err != nil {
		g.String(http.StatusBadRequest, err.Error())
		return
	}
}

func (a *Api) streamSubscribe(g *gin.Context) {
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
	fmt.Printf("Try to subscribe stream %s.\n", streamId)
	err = a.Node.SubscribeStream(streamId)
	if err != nil {
		g.String(http.StatusBadRequest, err.Error())
		return
	}

}

// Printout stat of current workers.
func (a *Api) streamWorkerStat(g *gin.Context) {
	a.Node.StreamWorkerStat()
}

func (a *Api) streamClose(g *gin.Context) {
	args, err := a.readArgs(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	if len(args) == 0{
		g.String(http.StatusBadRequest, "missing streamId")
		return
	}
	streamId:= args[0]

	fmt.Printf("Try to close stream %s", streamId)
	a.Node.StreamClose(streamId)
}
