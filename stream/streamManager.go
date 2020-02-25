package stream

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"

	//"github.com/libp2p/go-libp2p-core/peer"

	//"github.com/SJTU-OpenNetwork/hon-textile/core"
)

// StreamManager is used to handle stream requests.
// Note:
//		Manager has nothing to do with the creation of stream or the store of stream.
type StreamManager struct {
	blockFetcher func(streamId string, startIndex uint64, maxNum int) ([]cid.Cid, error)
	streamFetcher func(streamId string) (*pb.Stream, error)
	activeStreams map[string] *pb.Stream			// Cache active streams. (Maybe it is redundant.)
	activeWorkers map[string] []*streamWorker	// Cache active workers so we can send signals to them.
	newFile chan *workerSignal
}

// StreamWorker is used to
type streamWorker struct {
	stream *pb.Stream // Contains core information such as substream number
}

type workerSignal struct {
	stream string
}

func (sm *StreamManager) NewFileAdd() {

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

func (sw *streamWorker) Start() error {
	// Start the block sending routine
	return nil
}