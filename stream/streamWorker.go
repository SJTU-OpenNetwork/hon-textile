package stream

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
)

const maxBlockFetchNum = 20

// StreamWorker is used do blocksending task.
// Each streamrequest will create a independent worker.
// Note:
//		If a peer request two different substream of the same stream with two seperate request,
//		StreamManager will create two independent worker.
//		However, if a peer request two substream with one single request, there will be only one worker created.
type streamWorker struct {
	stream *pb.StreamMeta	// Contains stream info
	req *pb.StreamRequest 	// Contains core information such as substream and index
	pid peer.ID				// Contains information about destination
	currentIndex uint64		// The index of block sending now
	workSignal chan interface{}
	blockFetcher func(streamId string, startIndex uint64, maxNum int) ([] *pb.StreamBlock, error)
	blockSender func (destination peer.ID, streamBlk [] *pb.StreamBlock) error
	//stopSignal chan interface{}
}

func newStreamWorker(
	stream *pb.StreamMeta,
	pid peer.ID,
	req *pb.StreamRequest,
	blockFetcher func(streamId string, startIndex uint64, maxNum int) ([] *pb.StreamBlock, error),
	blockSender func (destination peer.ID, streamBlk [] *pb.StreamBlock) error) *streamWorker{

		return &streamWorker{
			stream: stream,
			req: req,
			pid: pid,
			currentIndex: req.StartIndex,
			workSignal: make(chan interface{}, 1),
			blockFetcher: blockFetcher,
			blockSender: blockSender,
		}
}

func (sw *streamWorker) notice() {
	// workSignal has buffer size 1.
	// notice() would not block if the worker has already been noticed.
	select{
		case sw.workSignal <- struct{}{}:
		default:
	}
}

func (sw *streamWorker) start() error {
	// Start the block sending routine
	sw.currentIndex = sw.req.StartIndex
	go func(){
		for {
			<-sw.workSignal
			// Do sending
			// Block if there is no signal
			blks, _ := sw.blockFetcher(sw.req.Id, sw.currentIndex, maxBlockFetchNum)
			if blks != nil {
				fblks := sw.filterBlocks(blks)
				// TODO: Unhandled error
				sw.blockSender(sw.pid, fblks)

				// Notice the worker again if there maybe more blocks can be fetched.
				if len(blks) >= maxBlockFetchNum {
					sw.notice()
				}
			}
			// TODO: How to end a worker???
		}
	}()
	return nil
}


// filter blocks to find the blocks belongs to certain substream.
func (sw *streamWorker) filterBlocks(blks []*pb.StreamBlock) []*pb.StreamBlock {
	streamMap := sw.req.StreamMap
	res := make([]*pb.StreamBlock, 0)
	for _, blk := range blks {
		subIndex := blk.Index % uint64(sw.stream.Nsubstreams)
		subMap := uint64(1) << subIndex
		if subMap & streamMap != 0 {
			res = append(res, blk)
		}
	}
	return res
}

func (sw *streamWorker) isSame(pid peer.ID, req *pb.StreamRequest) bool {
	return false
}


