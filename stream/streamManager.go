package stream

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	ipld "github.com/ipfs/go-ipld-format"
	//cmap "github.com/orcaman/concurrent-map"

)

var ErrRedundantReq = fmt.Errorf("Request is redundant")

// StreamManager is used to handle stream requests.
// How to use:
// 	- Give blockFetcher, streamFetcher, and blockSender when initialize a StreamManager.
//	- Call NewFileAdd() when you add a new file to some stream.
//	- Call ResponseRequest() when you would like to send blocks requested by some requestor.
//	- Call NewBlockReceive() when received a new block.
//	- Handle received file output from ReceivedFile.
// Note:
//		Manager has nothing to do with the creation of stream or the store of stream.
// TODO:
// 		1. Use Interface instead of callback function to do specific tasks.
//		2. Whether we should output streaminfo from ReceivedFile.
//		3. Let worker able to broadcast to multipeer
//		4. [vital] A method to stop worker  - How to distinguish old worker?
type StreamManager struct {
	blockFetcher func(streamId string, startIndex uint64, maxNum int) ([] *pb.StreamBlock, error)
	streamFetcher func(streamId string) (*pb.StreamMeta, error)
	blockSender func (destination peer.ID, streamBlk []*pb.StreamBlock) error
	//activeStreams cmap.ConcurrentMap	// Contains *pb.Stream. Cache active streams. (Maybe it is redundant.)
										// active
	activeWorkers *workerStore
	//newFile chan *workerSignal
	ReceivedFile <- chan ipld.Node
}

func NewStreamManager(
	bFetcher func(streamId string, startIndex uint64, maxNum int) ([]*pb.StreamBlock, error),
	sFetcher func(streamId string) (*pb.StreamMeta, error),
	bSender func (destination peer.ID, streamBlk []*pb.StreamBlock) error) *StreamManager {
	return &StreamManager{
		blockFetcher:bFetcher,
		streamFetcher:sFetcher,
		blockSender:bSender,
		//activeStreams: cmap.New(),
		activeWorkers: newWorkerStore(),
	}
}

// Note:
// 		That interface is not used yet.
//		It shows an example if we want to use interface replacing callback function.
type StreamManagerService interface {
	FetchBlock(streamId string, startIndex uint64, maxNum int) ([] *pb.StreamBlock, error)
	FetchStream(streamId string) (*pb.StreamMeta, error)
	SendBlock(destination peer.ID, streamBlk [] *pb.StreamBlock) error
}


func (sm *StreamManager) createWorker(pid peer.ID, req *pb.StreamRequest) *streamWorker {
	stream, err := sm.streamFetcher(req.Id)
	if err != nil {
		return nil
	}
	return newStreamWorker(stream, pid, req, sm.blockFetcher, sm.blockSender)
}


func (sm *StreamManager) NewFileAdd(streamId string) {

	sm.activeWorkers.newFileAdd(streamId)
}

// Note:
//		Be sure to avoid routine blocking when implementing this function.
func (sm *StreamManager) NewBlockReceive(streamBlk *pb.StreamBlock, data []byte) {
	// Call ipfs.DataAtPath when receive a root node.
}

// Call it when you decide to send blocks to requestor.
// Use "Response" to distinguish with "Handle".
func (sm *StreamManager) ResponseRequest(pid peer.ID, req *pb.StreamRequest) error {
	//_, ok := sm.activeStreams.Get(req.Id)

	//if !ok {
	//	stream, err := sm.streamFetcher(req.Id)
	//	if err != nil {
	//		return err
	//	}
	//	sm.activeStreams.Set(req.Id, stream)
	//}

	// Raise an error if obtain the same request (Same requestor with same streamid and substream requested by the same peer)
	if sm.activeWorkers.isRedundant(pid, req) {
		return ErrRedundantReq
	}
	worker := sm.createWorker(pid, req)
	// add worker to activeworkers
	sm.activeWorkers.add(worker)
	// start worker
	return worker.start()
}

