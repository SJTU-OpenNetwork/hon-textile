// Service for sending/receving stream related data - add by Jerry 2020/02/25

package stream

import (
	"encoding/json"
	"fmt"
	"github.com/SJTU-OpenNetwork/interface-go-ipfs-core/path"
	"io/ioutil"

	"bytes"
	"context"
	"github.com/segmentio/ksuid"
	"time"

	"github.com/SJTU-OpenNetwork/go-ipfs/core"
	"github.com/golang/protobuf/ptypes"
	ipld "github.com/ipfs/go-ipld-format"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	//	mh "github.com/multiformats/go-multihash"
	//	"github.com/segmentio/ksuid"
	//	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
	//	"github.com/SJTU-OpenNetwork/hon-textile/crypto"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/SJTU-OpenNetwork/hon-textile/keypair"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	//	"github.com/SJTU-OpenNetwork/hon-textile/repo/db"
	"github.com/SJTU-OpenNetwork/hon-textile/service"
	logging "github.com/ipfs/go-log"
)


// streamServiceProtocol is the current protocol tag
const streamServiceProtocol = protocol.ID("/textile/stream/1.0.0")
//const maxWorkers = 2
const defaultMaxWorkers = 2
var maxWorkers int
var log = logging.Logger("stream")
var ErrRedundantReq = fmt.Errorf("Request is redundant")
var ErrUnknowkStream = fmt.Errorf("Unknown stream")

type StreamService struct {
	service          *service.Service
	datastore        repo.Datastore
	online           bool
	sendNotification func(*pb.Notification) error
    subscribe        func(string) error 
	activeStreams *activeStreamStore
	
    // for workers
    activeWorkers *workerStore
	ReceivedFile <- chan ipld.Node
    
    // for providers
    providers *providerStore
	lostIndex chan *lostReport

	// Context for main routine
	ctx context.Context
}

// NewStreamService returns a new stream service
func NewStreamService(
	account *keypair.Full,
	node func() *core.IpfsNode,
	datastore repo.Datastore,
	sendNotification func(*pb.Notification) error,
    subscribe func(string) error,
	ctx context.Context,
) *StreamService {
	handler := &StreamService{
		datastore:        datastore,
		sendNotification: sendNotification,
        subscribe:        subscribe,
		ctx:			  ctx,
		//activeStreams:newActiveStreamStore(ctx, datastore, node, ),
		activeWorkers: newWorkerStore(),
        providers: newProviderStore(),
        
        // informations for started streams
	}
	handler.activeStreams = newActiveStreamStore(ctx, datastore, node, handler.activeWorkers.newFileAdd)
	handler.service = service.NewService(account, handler, node)
	return handler
}

// Protocol returns the handler protocol
func (h *StreamService) Protocol() protocol.ID {
	return streamServiceProtocol
}

// Start begins online services
func (h *StreamService) Start() {
	maxWorkers = defaultMaxWorkers
    h.online = true
	h.service.Start()
    // TODO:
    // 		It may not be a good idea to use StreamService as StreamNotifee directly.
	h.service.Node().PeerHost.Network().Notify((*StreamNotifee)(h))
}

// Ping pings another peer
func (h *StreamService) Ping(pid peer.ID) (service.PeerStatus, error) {
	return h.service.Ping(pid.Pretty())
}

// Handle is called by the underlying service handler method
func (h *StreamService) Handle(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	fmt.Printf("core/stream_service.go Handler: New message receive from %s.\n", pid.Pretty())
	switch env.Message.Type {
	case pb.Message_STREAM_BLOCK:
		return h.handleStreamBlock(env, pid)
	case pb.Message_STREAM_BLOCK_LIST:
		return h.handleStreamBlockList(env, pid)
	case pb.Message_STREAM_REQUEST:
		return h.handleStreamRequest(env, pid)
	case pb.Message_STREAM_UNSUBSCRIBE:
		return h.handleUnsubscribe(env, pid)
    default:
    	fmt.Printf("core/stream_service.go Handler: Unknown message type")
        return nil, nil
    }
}

// ======================= FOR STREAM MANAGEMENT =============================
func (h *StreamService) StartStream(config *pb.StreamMeta) {
	err := h.activeStreams.addStream(config)
	if err != nil {
		log.Error(err)
	}
	selfPeerId := h.service.Node().Identity.Pretty()
	acceptedSubstream := newProvidedSubstream(config.Id, 1, 1, 0, selfPeerId, h.handleBlockLost)
	provider := h.providers.getOrCreate(selfPeerId)
	provider.add(acceptedSubstream)
}

func (h *StreamService) SetMaxWorkers(n int) {
	log.Debugf("Change max workers to %d", n)
	maxWorkers = n
}

func (h *StreamService) GetMaxWorkers() int {
	return maxWorkers
}

/**
 * Started return true if there stream with id "sid" is working.
 */
func (h *StreamService) Started(sid string) bool{
    //_, ok:= h.streamFileChannels[sid]
    return h.activeStreams.isActive(sid)
}

func (h *StreamService) StreamAddFile(id string, sf *pb.StreamFile) error{
	err := h.activeStreams.streamAddFile(id, sf); if err != nil {log.Error(err);return err}
    return nil
}

func (h *StreamService) CloseStream(sid string) {
    fmt.Printf("StreamService: Try to close stream %s\n", sid)
    err := h.activeStreams.stopStream(sid); if err != nil {log.Error(err)}
    // remove self provider
    h.providers.RemoveStream(sid)
}

// UnsubscribeStream want to unsubscribe to a stream, and send a request to the
// provider.
func (h *StreamService) UnsubscribeStream(sid string) error{
    fmt.Printf("StreamService: Try to unsubscribe stream %s\n", sid)
    pids := h.providers.RemoveStream(sid)
    for _, p := range pids {
    	_, err := h.SendUnsubscribeRequest(p.Pretty(), sid)
    	if err != nil {
    		return err
		}
	}
    return nil
}

// ======================== FOR MESSAGE RECV/SEND ==================================
// handleStreamBlock receives a STREAM_BLOCK message [deprecated]
func (h *StreamService) handleStreamBlock(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	fmt.Printf("StreamService: New stream blk receive from %s\n", pid.Pretty())
    blk := new(pb.StreamBlockContent)
    err := ptypes.UnmarshalAny(env.Message.Payload, blk)
    if err != nil {
        return nil, err
    }

    stat, err := ipfs.PutBlock(h.service.Node(), bytes.NewReader(blk.Data))
    if err != nil {
        return nil, err
    }
    cid := stat.Path().Cid()
    model := &pb.StreamBlock {
        Id: cid.String(),
        Streamid: blk.StreamID,
        Index: blk.Index,
    }

	log.Debugf("[%s] Block %s, Stream %s, From %s, Size %d", TAG_BLOCKRECEIVE, cid.String(), blk.StreamID, pid.Pretty(), stat.Size())
    err = h.datastore.StreamBlocks().Add(model)
    return nil, err
}


// handleStreamBlock receives a STREAM_BLOCK_LIST message
func (h *StreamService) handleStreamBlockList(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	//fmt.Printf("StreamService: New stream blk list receive from %s\n", pid.Pretty())
    streams := make(map[string]int)
    blks := new(pb.StreamBlockContentList)
    err := ptypes.UnmarshalAny(env.Message.Payload, blks)
    if err != nil {
        return nil, err
    }
    for _, blk := range blks.Blocks {
        size := 0
        cid_str := ""
        if len(blk.Data) != 0 {
            stat, err := ipfs.PutBlock(h.service.Node(), bytes.NewReader(blk.Data))
            if err != nil {
                return nil, err
            }
            cid := stat.Path().Cid()
            cid_str = cid.String()
        }
        model := &pb.StreamBlock {
            Id: cid_str,
            Streamid: blk.StreamID,
            Index: blk.Index,
            Size: int32(size),
            IsRoot: blk.IsRoot,
            Description: string(blk.Description),
        }
        //fmt.Printf("StreamService: Received stream %s; index %d; cid %s\n", blk.StreamID, blk.Index, cid.String())
        log.Debugf("[%s] Block %s, Stream %s, Index %d, From %s, Size %d", TAG_BLOCKRECEIVE, cid_str, blk.StreamID, blk.Index, pid.Pretty(), size)
        err = h.datastore.StreamBlocks().Add(model)
        if err != nil {
            return nil, err
        }
        //fmt.Printf("It is successfully stored in our database!\n")

        if blk.IsRoot {
            // we found a file !
            fmt.Print("It is a root node of a merkle-DAG!\n")
            err = h.handleRootBlk(pid, model)
            if err != nil {
                fmt.Printf("Handle root file failed\n")
                return nil, err
            }
        }
        streams[blk.StreamID] = 1
    }
    for id := range streams {
	    err := h.activeWorkers.newFileAdd(id)
	    if err != nil {
	    	log.Error(err)
		}
    }
    return nil, nil
}

// handleRootBlk does following works
//		- Send Notification to application
//		- Update number of blocks in streammeta datastore
func (h *StreamService) handleRootBlk(pid peer.ID, blk *pb.StreamBlock) error {
	var body string
	if blk.Id != "" {
		meta := h.datastore.StreamMetas().Get(blk.Streamid)
		if meta == nil {
			log.Errorf("No stream meta for stream %s root block %s", blk.Streamid, blk.Id)
			body = "unknown stream meta"
		} else {
			switch meta.Type {
			case pb.StreamMeta_FILE:
				body = "stream file"
			case pb.StreamMeta_PICTURE:
				body = "stream picture"
			case pb.StreamMeta_VIDEO:
				body = "stream video"
			}
		}
	}
    pdate, _ := ptypes.TimestampProto(time.Now())
	note := &pb.Notification{
		Id:          ksuid.New().String(),
		Date:        pdate,
		Actor:       pid.Pretty(),
		Subject:     blk.Streamid,
		SubjectDesc: blk.Description,
		Block:       blk.Id,
		Target:      "",
        Type:        pb.Notification_STREAM_FILE,
		Body:        body,
	}
    err := h.sendNotification(note)
	if err != nil {
		return err
	}

    if blk.Id == "" {
    	log.Debugf("[%s] Stream %s", TAG_STREAM_COMPLETE, blk.Streamid)
        meta := h.datastore.StreamMetas().Get(blk.Streamid)
	    if meta == nil || meta.Nblocks > 0{
			log.Errorf("No stream meta for stream %s root block %s", blk.Streamid, blk.Id)
		    return nil
	    }
        err := h.datastore.StreamMetas().UpdateNblocks(blk.Streamid, blk.Index)
        if err != nil {
            log.Error(err)
            return err
        }
        // Remove provider here
		h.providers.RemoveStream(blk.Streamid)
    }
    return nil
}


// HandleStream is called by the underlying service handler method
func (h *StreamService) HandleStream(env *pb.Envelope, pid peer.ID) (chan *pb.Envelope, chan error, chan interface{}) {
	return make(chan *pb.Envelope), make(chan error), make(chan interface{})
}

// SendMessage sends a message to a peer.
func (h *StreamService) SendMessage(ctx context.Context, peerId string, env *pb.Envelope) error {
	return h.service.SendMessage(ctx, peerId, env)
}

func (h *StreamService) IsBusy() bool {
	numWorkers := h.Workload()
	return numWorkers >= maxWorkers
}

// HandleRequest
func (h *StreamService) handleStreamRequest(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	fmt.Printf("core/stream_service.go handleStreamRequest from %s\n", pid.Pretty())
	req := new(pb.StreamRequest)
	err := ptypes.UnmarshalAny(env.Message.Payload, req)
	if err != nil {
        //fmt.Printf(err)
		return nil, err
	}

	//return h.service.NewEnvelope(pb.Message_STREAM_REQUEST_HANDLE, &pb.StreamRequestHandle{
	//    Value:1,
	//},nil, true)
    
    // TODO: calculate capacity according to video rate
	numWorkers := h.Workload()
    if numWorkers < maxWorkers {
    	log.Debugf("[%s], Stream %s, To %s, Workers %d", TAG_STREAMREQUESTACCEPT, req.Id, pid.Pretty(), numWorkers)
        err = h.responseRequest(pid, req)
        if err != nil {
            return nil, err
        }
        return h.service.NewEnvelope(pb.Message_STREAM_REQUEST_HANDLE, &pb.StreamRequestHandle{
    	    Value:1,
        },nil, true)
    } else {
		log.Debugf("[%s], Stream %s, To %s, Workers %d", TAG_STREAMREQUESTREJECT, req.Id, pid.Pretty(), numWorkers)
        return h.service.NewEnvelope(pb.Message_STREAM_REQUEST_HANDLE, &pb.StreamRequestHandle{
    	    Value:0,
        },nil, true)
    }
}

func (h *StreamService) SendStreamRequest(peerId string, config *pb.StreamRequest) (*pb.Envelope, error) {
	//fmt.Printf("core/stream_service.go SendStreamRequest to %s\n", peerId)
	env, err := h.service.NewEnvelope(pb.Message_STREAM_REQUEST, config, nil, false)
	if err != nil {
		return nil,err
	}
	log.Debugf("[%s] Stream %s, To %s", TAG_STREAMREQUEST, config.Id, peerId)
	return h.service.SendRequest(peerId, env)
}

func (h *StreamService) SendUnsubscribeRequest(peerId string, sid string) (*pb.Envelope, error) {
	//fmt.Printf("core/stream_service.go SendStreamRequest to %s\n", peerId)
	env, err := h.service.NewEnvelope(pb.Message_STREAM_UNSUBSCRIBE, &pb.StreamUnsubscribe{
        Id: sid,
    }, nil, false)
	if err != nil {
		return nil,err
	}
	//log.Debugf("[%s] Stream %s, To %s", TAG_STREAMREQUEST, sid, peerId)
	return h.service.SendRequest(peerId, env)
}

func (h *StreamService) handleUnsubscribe(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	fmt.Printf("core/stream_service.go handleUnsubscribe from %s\n", pid.Pretty())
	req := new(pb.StreamUnsubscribe)
	err := ptypes.UnmarshalAny(env.Message.Payload, req)
	if err != nil {
		return nil, err
	}
    
    //TODO: stop sending data to pid
    h.activeWorkers.endWorker(req.Id, pid.Pretty())

	return h.service.NewEnvelope(pb.Message_STREAM_UNSUBSCRIBE_RES, &pb.StreamUnsubscribeAck{
        Id:req.Id,
    }, nil, true)
}

// RequestAccepted is called when a stream request is accepted by some peer.
func (h *StreamService) RequestAccepted(peerId string, config *pb.StreamRequest) {
	log.Debugf("[%s] Stream %s, By %s", TAG_STREAM_REQUEST_ACCEPTED, config.Id, peerId)
	acceptedSubstream := newProvidedSubstream(config.Id, config.StreamMap, 1, config.StartIndex, peerId, h.handleBlockLost)
	provider := h.providers.getOrCreate(peerId)
	// TODO: De-duplicated
	provider.add(acceptedSubstream)
}

// Handle lost block
// It would be called by providedSubstream.
func (h *StreamService) handleBlockLost(report *lostReport){
	fmt.Println("BlockLost")
	infos :=  report.Loggable()
	js, err := json.MarshalIndent(infos, "", "  ")
	if err != nil {
		fmt.Printf("%s\n", err.Error())
	} else {
		fmt.Printf("%s\n", string(js))
	}
}

// Call it when you decide to send blocks to requestor.
// Use "Response" to distinguish with "Handle".
func (h *StreamService) responseRequest(pid peer.ID, req *pb.StreamRequest) error {
	log.Debugf("[%s] Stream %s, From %s", TAG_STREAMRESPONSE, req.Id, pid.Pretty())

	// Raise an error if obtain the same request (Same requestor with same streamid and substream requested by the same peer)
	if h.activeWorkers.isRedundant(pid, req) {
		return ErrRedundantReq
	}
	worker, err := h.createWorker(pid, req)
	if err != nil {
		return err
	}
	// add worker to activeworkers
	err = h.activeWorkers.add(worker)
	if err != nil {
		return err
	}
	// start worker
	return worker.start()
}

//SendStreamBlocks send a list of block to a peer
func (h *StreamService) SendStreamBlocks(peerId peer.ID, blks []*pb.StreamBlock) error{
	//fmt.Printf("StreamService: Send %d stream blks to %s\n", len(blks), peerId.Pretty())

	// Marshal blocks to pb
    blist := new(pb.StreamBlockContentList)
    for _, blk:= range blks {
        var data []byte
        if blk.Id != "" {
            r, err := ipfs.GetBlock(h.service.Node(), path.New(blk.Id))
            data, err = ioutil.ReadAll(r)
		    if err != nil {
                log.Error(err)
			    return err
		    }
        }

        content := &pb.StreamBlockContent{
            StreamID: blk.Streamid,
            Index: blk.Index,
            Data: data,
            IsRoot: blk.IsRoot,
            Description: []byte(blk.Description),
        }
        log.Debugf("[%s] Isroot %t, Block %s, Stream %s, Index %d, To %s, Size %d, description: %s", TAG_BLOCKSEND, blk.IsRoot, blk.Id, blk.Streamid, blk.Index, peerId.Pretty(), blk.Size, blk.Description)
        blist.Blocks = append(blist.Blocks, content)
    }
	env, err := h.service.NewEnvelope(pb.Message_STREAM_BLOCK_LIST, blist, nil, false)
	if err != nil {
        log.Error(err)
		return err
	}
	// Send envelope use StreamService.service.SendMessage
    err = h.service.SendMessage(nil, peerId.Pretty(), env)
    if err != nil {
    	var ind1, ind2 uint64
    	var streamId1 string
    	if len(blks) > 0 {
    		ind1 = blks[0].Index
    		ind2 = blks[len(blks)-1].Index
    		streamId1 = blks[0].Streamid
		}
		log.Debugf("[%s] Stream %s, Index %v - %v, To %s", TAG_BLOCKSEND_FAILED, streamId1, ind1, ind2, peerId.Pretty())
        log.Error(err)
    	return err
    }
	return nil
}

// FetchBlocks fetches a list of blocks of a specific stream from database
func (h *StreamService) FetchBlocks(streamId string, startIndex uint64, maxNum int) ([]*pb.StreamBlock, error){
    // find blocks of the stream with id = streamId
    blks := h.datastore.StreamBlocks().ListByStream(streamId, int(startIndex),maxNum)
	if blks == nil{
		return nil,fmt.Errorf("stream blocks fetch failed")
	}
    return blks, nil
}

func (h *StreamService) AddPotential(pid string, config *pb.StreamRequest, hopcnt int) {
}


// =============== FOR WORKDERS ==================
func (h *StreamService) createWorker(pid peer.ID, req *pb.StreamRequest) (*streamWorker, error) {
	//fmt.Printf("stream/streamManager createWorker")
    stream := h.datastore.StreamMetas().Get(req.Id)
	if stream == nil {
		return nil, ErrUnknowkStream
	}
	return newStreamWorker(stream, pid, req, h.FetchBlocks, h.SendStreamBlocks), nil
}

func (h *StreamService) Workload() int {
    // return the required bitrate for all workers
    // for now, just return the number of workers
    return h.activeWorkers.Workload()
}

func (h *StreamService) WorkerStat() {
	log.Debugf("StreamManager.WorkerStat()\n")
	h.activeWorkers.PrintOut()
}


// ============== FOR PEER MANAGEMENT ===================
func (h *StreamService)PeerDisconnected(pid peer.ID) {
	// Stop all the workers
	log.Debugf("Peer %s disconnected", pid)
	h.activeWorkers.endPeer(pid.Pretty())
    provider := h.providers.remove(pid.Pretty())
    if provider != nil{
        // re-subscribe streams
        for _,stream := range(provider.subStreams) {
            err := h.subscribe(stream.streamId)
            if err != nil{
                log.Error(err)
            }
        }
    }
}

func (h* StreamService) GetProvidedHopcnt(config *pb.StreamRequest) (int, bool){
	return 0, false
}

func (h* StreamService) GetProvider(sid string) peer.ID{
	return h.providers.GetProvider(sid)
}
// ===================== OTHERS =========================
func (h *StreamService) Loggable() map[string]interface{}{
	return h.activeWorkers.Loggable()
}

