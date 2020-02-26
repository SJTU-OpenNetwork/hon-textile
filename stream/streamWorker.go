package stream

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
)

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

func (sw *streamWorker) start() error {
	// Start the block sending routine

	return nil
}
