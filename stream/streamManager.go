package stream

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
    "github.com/SJTU-OpenNetwork/go-ipfs/core"
	//"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	ipld "github.com/ipfs/go-ipld-format"
	//cmap "github.com/orcaman/concurrent-map"
	logging "github.com/ipfs/go-log"

)

var ErrRedundantReq = fmt.Errorf("Request is redundant")
var ErrUnknowkStream = fmt.Errorf("Unknown stream")
var log = logging.Logger("stream")

const (
    MAXNSUBSTREAM = 5
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
//		3. Let worker able to broadcast to multipeer
//		4. [vital] A method to stop worker  - How to distinguish old worker?
type StreamManager struct {
	blockFetcher func(streamId string, startIndex uint64, maxNum int) ([] *pb.StreamBlock, error)
	streamFetcher func(streamId string) (*pb.StreamMeta, error)
	blockSender func (destination peer.ID, streamBlk []*pb.StreamBlock) error
	Node    func() *core.IpfsNode
	//activeStreams cmap.ConcurrentMap	// Contains *pb.Stream. Cache active streams. (Maybe it is redundant.)
										// active
	activeWorkers *workerStore
	//newFile chan *workerSignal
	ReceivedFile <- chan ipld.Node
    providers *providerStore
    
    streamFileChannels map[string]chan *pb.StreamFile
    streamBlockIndex map[string]uint64
    streamDone map[string] bool
	streamlock sync.Mutex
}

func NewStreamManager(
	bFetcher func(streamId string, startIndex uint64, maxNum int) ([]*pb.StreamBlock, error),
	sFetcher func(streamId string) (*pb.StreamMeta, error),
	bSender func (destination peer.ID, streamBlk []*pb.StreamBlock) error,
    Node func() *core.IpfsNode) *StreamManager{
	return &StreamManager{
		blockFetcher:bFetcher,
		streamFetcher:sFetcher,
		blockSender:bSender,
        Node: Node,
		//activeStreams: cmap.New(),
		activeWorkers: newWorkerStore(),
        providers: newProviderStore(),

        // informations for started streams
        streamFileChannels: make(map[string]chan *pb.StreamFile),
        streamBlockIndex: make(map[string] uint64) ,
        steamDone: make(map[string] bool,
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


func (sm *StreamManager) createWorker(pid peer.ID, req *pb.StreamRequest) (*streamWorker, error) {
	//fmt.Printf("stream/streamManager createWorker")
	stream, err := sm.streamFetcher(req.Id)
	if err != nil {
		log.Errorf(err.Error())
		return nil, ErrUnknowkStream
	}
	return newStreamWorker(stream, pid, req, sm.blockFetcher, sm.blockSender), nil
}


func (sm *StreamManager) NewFileAdd(streamId string) {
	sm.activeWorkers.newFileAdd(streamId)
}

func (sm *StreamManager) StartStream(config *pb.StreamMeta, newFileHandler func(*pb.StreamFile)) {
    sm.streamlock.Lock()
    defer sm.streamlock.Unlock()

    _, ok = sm.streamFileChannels[config.Id]
    if ok {
        // how to handle re-start?
        return
    }
    
    sm.streamBlockIndex[config.Id] = 0
    sm.streamFileChannels[config.Id] = make(chan *pb.StreamFile)
    sm.streamDone[config.Id] = false
	fmt.Printf("Start the add routine for stream\n")
    go func(){
		fmt.Printf("Stream routine start.\n")
	   for {
		    select {
            case  newfile := <-sm.streamFileChannels[config.Id]:
                newFileHandler(newfile)
                sm.NewFileAdd(config.Id)
            case sm.streamDone[config.Id]: //CloseStream is called
                //TODO what if there are files left in the channel?
                return
		    }
	   }
    }()
}

func (sm *StreamManager) CloseStream(sid string) {
    sm.streamlock.Lock()
    defer sm.streamlock.Unlock()
    
    sm.streamDone[config.Id] = true
    close sm.streamFileChannels[sid]
    delete (sm.streamFileChannels,sid)
    delete (sm.streamBlockIndex,sid)
}

func (sm *StreamManager) Started(sid string) bool{
    sm.streamlock.Lock()
    defer sm.streamlock.Unlock()
    
    _, ok:= sm.streamFileChannels[sid]
    return ok
}

// Note:
//		Be sure to avoid routine blocking when implementing this function.
func (sm *StreamManager) NewBlockReceive(streamBlk *pb.StreamBlock, data []byte) {
	// Call ipfs.DataAtPath when receive a root node.
}

func (sm *StreamManager) Workload() int {
    // return the required bitrate for all workers
    // for now, just return the number of workers
    return sm.activeWorkers.Workload()
}

func (sm *StreamManager) WorkerStat() {
	log.Debugf("StreamManager.WorkerStat()\n")
	sm.activeWorkers.PrintOut()
}

func (sm *StreamManager) Loggable() map[string]interface{}{
	return sm.activeWorkers.Loggable()
}

func (sm *StreamManager)PeerDisconnected(pid peer.ID) {
	// Stop all the workers
	log.Debugf("Peer %s disconnected", pid)
	sm.activeWorkers.endPeer(pid.Pretty())
    sm.providers.peerDisconnected(pid)
}

// Call it when you decide to send blocks to requestor.
// Use "Response" to distinguish with "Handle".
func (sm *StreamManager) ResponseRequest(pid peer.ID, req *pb.StreamRequest) error {
	//fmt.Printf("stream/streamManager.go ResponseRequest\n")
	log.Debugf("[%s] Stream %s, From %s", TAG_STREAMRESPONSE, req.Id, pid.Pretty())
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
	worker, err := sm.createWorker(pid, req)
	if err != nil {
		return err
	}
	// add worker to activeworkers
	err = sm.activeWorkers.add(worker)
	if err != nil {
		return err
	}
	// start worker
	return worker.start()
}

func (sm *StreamManager) GetProvider(config *pb.StreamRequest) *Provider {
    return sm.providers.getProvider(config)
}
