package stream

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/orcaman/concurrent-map"
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
	blockFetcher func(streamId string, startIndex uint64, maxNum int) ([]cid.Cid, error)
	streamFetcher func(streamId string) (*pb.Stream, error)
	blockSender func (destination peer.ID, streamBlk *pb.StreamBlock) error
	//activeStreams cmap.ConcurrentMap	// Contains *pb.Stream. Cache active streams. (Maybe it is redundant.)
										// active
	activeWorkers cmap.ConcurrentMap	// Contains another ConcurrentMap of worker. Cache active workers so we can send signals to them.
										// activeWorkers [streamid] [peerid] worker
	newFile chan *workerSignal
	ReceivedFile <- chan ipld.Node
}

func NewStreamManager(
	bFetcher func(streamId string, startIndex uint64, maxNum int) ([]cid.Cid, error),
	sFetcher func(streamId string) (*pb.Stream, error),
	bSender func (destination peer.ID, streamBlk *pb.StreamBlock) error) *StreamManager {
	return &StreamManager{
		blockFetcher:bFetcher,
		streamFetcher:sFetcher,
		blockSender:bSender,
		//activeStreams: cmap.New(),
		activeWorkers: cmap.New(),
	}
}

// Note:
// 		That interface is not used yet.
//		It shows an example if we want to use interface replacing callback function.
type StreamManagerService interface {
	FetchBlock(streamId string, startIndex uint64, maxNum int) ([] *pb.StreamBlock, error)
	FetchStream(streamId string) (*pb.Stream, error)
	SendBlock(destination peer.ID, streamBlk [] *pb.StreamBlock) error
}

// StreamWorker is used do blocksending task.
// Each streamrequest will create a independent worker.
// Note:
//		If a peer request two different substream of the same stream with two seperate request,
//		StreamManager will create two independent worker.
//		However, if a peer request two substream with one single request, there will be only one worker created.
type streamWorker struct {
	 req *pb.StreamRequest 	// Contains core information such as substream and index
	 pid peer.ID			// Contains information about destination
}

type workerSignal struct {
	stream string
}

func (sm *StreamManager) NewFileAdd(streamId string) {
	sm.newFile <- &workerSignal{stream: streamId}
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
	if sm.isRedundantReq(pid, req) {
		return ErrRedundantReq
	}
	worker := &streamWorker{req: req, pid: pid}

	// add worker to activeworkers

	// start worker
	return worker.start()

}



func (sw *streamWorker) start() error {
	// Start the block sending routine
	
	return nil
}

// isRedundantReq judge whether a request is redundant
func (sm *StreamManager) isRedundantReq(pid peer.ID, req *pb.StreamRequest) bool {
	return false
}