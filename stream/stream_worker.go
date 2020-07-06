package stream

import (
	"context"
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	"github.com/SJTU-OpenNetwork/hon-textile/util"
	"github.com/libp2p/go-libp2p-core/peer"
	"time"
)

const maxBlockFetchNum = 1

// StreamWorker is used do blocksending task.
// Each streamrequest will create a independent worker.
// Note:
//		If a peer request two different substream of the same stream with two seperate request,
//		StreamManager will create two independent worker.
//		However, if a peer request two substream with one single request, there will be only one worker created.
type streamWorker struct {
	ctx context.Context
	cancelFunc context.CancelFunc
	stream *pb.StreamMeta	// Contains stream info
	req *pb.StreamRequest 	// Contains core information such as substream and index
	pid peer.ID				// Contains information about destination
	currentIndex uint64		// The index of block sending now
    end bool
	workSignal chan interface{}
	blockFetcher func(streamId string, startIndex uint64, maxNum int) ([] *pb.StreamBlock, error)
	blockSender func (destination peer.ID, streamBlk [] *pb.StreamBlock) error
	//stopSignal chan interface{}
	taskQueue *util.TaskQueue
}

func newStreamWorker(
	ctx context.Context,
	stream *pb.StreamMeta,
	pid peer.ID,
	req *pb.StreamRequest,
	blockFetcher func(streamId string, startIndex uint64, maxNum int) ([] *pb.StreamBlock, error),
	blockSender func (destination peer.ID, streamBlk [] *pb.StreamBlock) error,
	queue *util.TaskQueue) *streamWorker{
		newCtx, cancelFunc := context.WithCancel(ctx)
		return &streamWorker{
			ctx: newCtx,
			cancelFunc: cancelFunc,
			stream: stream,
			req: req,
			pid: pid,
			currentIndex: req.StartIndex,
			workSignal: make(chan interface{}, 1),
			blockFetcher: blockFetcher,
			blockSender: blockSender,
            end: false,
            taskQueue: queue,
		}
}

type sendTask struct {
	worker *streamWorker
	blocks []*pb.StreamBlock
	retry int
	retryChan chan *sendTask
}

func (t *sendTask) Execute() {
	//log.Debugf("Execute send task for to ", t.worker.pid.Pretty())
	err := t.worker.blockSender(t.worker.pid, t.blocks)
	if err != nil {
		recorder.Hlog.Add(fmt.Sprintf("Send error for %d blocks. %v", len(t.blocks), err))
		t.retry--
		t.retryChan <- t
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

// cancel worker
// Note:
// 		cancel have nothing to do with workerstore.
func (sw *streamWorker) cancel(){
	sw.cancelFunc()
}

func (sw *streamWorker) start() error {
	//log.Debugf("[%s] Stream %s, To %s", TAG_WORKERSTART, sw.stream.Id, sw.pid.Pretty())
	//fmt.Printf("stream/streamWorker.go start(): Worker for stream %s to %s start\n", sw.stream.Id, sw.pid.Pretty())
	// Start the block sending routine
	sw.currentIndex = sw.req.StartIndex
	sw.notice() //notice once at begining
	retryChan := make(chan *sendTask, 5)

	go func(){
		//defer fmt.Printf("stream/streamWorker.go start(): worker for stream %s to %s end\n", sw.stream.Id, sw.pid.Pretty())
		for {
			select {
				case <-sw.workSignal:
					// Do sending
					// Block if there is no signal
					//log.Debug("Worker wake up.")
					//recorder.Hlog.Add("Worker wake up.")
					blks, _ := sw.blockFetcher(sw.req.Id, sw.currentIndex, maxBlockFetchNum)
					if blks != nil && len(blks) > 0 {
						//fmt.Printf("stream/streamWorker.go start(): send %d blks for stream %s to %s start\n", len(blks), sw.stream.Id, sw.pid.Pretty())
						fblks := sw.filterBlocks(blks)

						/*
						sTask := &sendTask{
							worker:    sw,
							blocks:    fblks,
							retry:     3,
							retryChan: retryChan,
						}
						err := sw.taskQueue.AddTask(sw.ctx, sTask)
						if err != nil {
							log.Error(err)
							recorder.Hlog.Add(fmt.Sprintf("Error when add send task: %v", err))
						}
						 */

						err := sw.blockSender(sw.pid, fblks)
						if err != nil {
							log.Errorf("Stream %s %v", sw.stream.Id, err)
							//log.Errorf("[%s] Stream %s, Index %v, To %s", TAG_BLOCKSEND_FAILED, )
							//log.Errorf("%s\nError occur when sending blocks.", err.Error())
                            time.Sleep(100*time.Millisecond) //something wrong, maybe the connection breaks, if that happens, the worker will be canceled
                            sw.notice()	// Resend block if the connection is still there
                            break
						}

						sw.currentIndex = sw.currentIndex + uint64(len(blks))
						if len(blks) >= maxBlockFetchNum {
							// Notice the worker again if there maybe more blocks can be fetched.
							sw.notice()
						}
                        if fblks[len(fblks)-1].Id == "" {
                            sw.cancel()
                        }
					}
					//log.Debug("Worker sleep.")
					//recorder.Hlog.Add("Worker sleep.")

				case <- sw.ctx.Done():
					// Note that break will break select only.
                    // log.Debug("worker task complete, call cancel")
					// log.Debugf("[%s] Stream %s, To %s", TAG_WORKEREND, sw.stream.Id, sw.pid.Pretty())
                    sw.end = true
					return

			}
		}
	}()

	/*
	 * The reason why a new routine is create to handle retry is to avoid deadlock.
	 * retryChan can be blocked if send task keep failed.
	 */
	go func() {
		select {
		case t, ok := <-retryChan: // Avoid to send to and receive from the same channel in the same routine
			//time.Sleep(100 * time.Millisecond) //something wrong, maybe the connection breaks, if that happens, the worker will be canceled
			if ok {
				if t.retry > 0 {
					recorder.Hlog.Add(fmt.Sprintf("Retry send task to %s", sw.pid.Pretty()))
					err := sw.taskQueue.AddTask(sw.ctx, t)
					if err != nil {
						log.Error(err)
						recorder.Hlog.Add(fmt.Sprintf("Error when add send task: %v", err))
					}
				} else {
					recorder.Hlog.Add(fmt.Sprintf("Send task failed to %s", sw.pid.Pretty()))
				}
			} else {
				recorder.Hlog.Add("Stream worker: retry channel is closed")
			}
		case <- sw.ctx.Done():
			// Note that break will break select only.
			// log.Debug("worker task complete, call cancel")
			log.Debugf("[%s] Stream %s, To %s", TAG_WORKEREND, sw.stream.Id, sw.pid.Pretty())
			sw.end = true
			return
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

// Convert basic info of worker to loggable map
//func (sw *streamWorker) Loggab<F11>le() map[string]interface{} {
//
//}


