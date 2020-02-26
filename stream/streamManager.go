package stream

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	ipld "github.com/ipfs/go-ipld-format"
)

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
type StreamManager struct {
	blockFetcher func(streamId string, startIndex uint64, maxNum int) ([]cid.Cid, error)
	streamFetcher func(streamId string) (*pb.Stream, error)
	blockSender func (destination peer.ID, streamBlk *pb.StreamBlock) error
	activeStreams map[string] *pb.Stream		// Cache active streams. (Maybe it is redundant.)
	activeWorkers map[string] []*streamWorker	// Cache active workers so we can send signals to them.
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
		activeStreams: make(map[string] *pb.Stream),
		activeWorkers: make(map[string] []*streamWorker),
	}
}

// Note:
// 		That interface is not used yet.
//		It shows an example if we want to use interface replacing callback function.
type StreamManagerService interface {
	FetchBlock(streamId string, startIndex uint64, maxNum int) ([]cid.Cid, error)
	FetchStream(streamId string) (*pb.Stream, error)
	SendBlock(destination peer.ID, streamBlk *pb.StreamBlock) error
}

// StreamWorker is used to
type streamWorker struct {
	stream *pb.Stream // Contains core information such as substream number
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

}

// Call it when you decide to send blocks to requestor.
// Use "Response" to distinguish with "Handle".
func (sm *StreamManager) ResponseRequest(pid peer.ID, req *pb.StreamRequest) error {
	_, ok := sm.activeStreams[req.Id]

	if !ok {
		stream, err := sm.streamFetcher(req.Id)
		if err != nil {
			return err
		}
		sm.activeStreams[req.Id] = stream
	}

	sm.startNewWorker(req)

	return nil
}

func (sm *StreamManager) startNewWorker(req *pb.StreamRequest) error {
	// Create a new worker and add it to activeWorkers.
	// Raise an error if obtain the same request (Same requestor with same streamid and substream)
	return nil
}

func (sw *streamWorker) start() error {
	// Start the block sending routine
	return nil
}