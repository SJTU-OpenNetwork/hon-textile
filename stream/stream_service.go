// Service for sending/receving stream related data - add by Jerry 2020/02/25

package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	honlog "github.com/SJTU-OpenNetwork/hon-textile/hon-log"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	"github.com/SJTU-OpenNetwork/hon-textile/util"
	"github.com/golang/protobuf/proto"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/segmentio/ksuid"
	"io/ioutil"
	"net"

	//"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/ipfs/go-ipfs/core"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
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
const defaultMaxTaskWorkers = 1
const InfoObsoleteTime = time.Hour * 5
const InformTimeOut = time.Second * 20
const RecvTimeout = time.Minute

var maxWorkers int
var maxTaskWorkers int
var log = logging.Logger("stream")
var ErrRedundantReq = fmt.Errorf("Request is redundant")
var ErrUnknowkStream = fmt.Errorf("Unknown stream")


type StreamMode int
const (
	StreamMode_PUSH	StreamMode = 0
	StreamMode_PULL StreamMode = 1
)


type StreamService struct {
	service          *service.Service
	datastore        repo.Datastore
	online           bool
	sendNotification func(*pb.Notification) error
    subscribe        func(string) error 
	activeStreams *activeStreamStore
	getShadow        func() string
	getShadowIp 	 func() string
	
    // for workers
    activeWorkers *workerStore
	ReceivedFile <- chan ipld.Node
    
    // for providers
    providedStreams *ProvidedStreams
	// for taskQueue
	taskQueue *util.TaskQueue
	// Context for main routine
	ctx context.Context

    streamInfos        *StreamInfos
	//streamInfosLock    sync.Mutex
	//
	recvBuf chan *recvTask

	// config
	mode StreamMode

	//cp *ConnPool

	tcpCon net.Conn
}

// NewStreamService returns a new stream service
func NewStreamService(
	account *keypair.Full,
	node func() *core.IpfsNode,
	datastore repo.Datastore,
	sendNotification func(*pb.Notification) error,
    subscribe func(string) error,
	getShadow func() string,
	getShadowIp func() string,
	ctx context.Context,
) *StreamService {
	handler := &StreamService{
		datastore:        datastore,
		sendNotification: sendNotification,
        subscribe:        subscribe,
		ctx:			  ctx,
		activeWorkers: newWorkerStore(),
        providedStreams: &ProvidedStreams{},
		taskQueue: util.NewTaskQueue(ctx, defaultMaxTaskWorkers),
		mode: StreamMode_PUSH,
		recvBuf: make(chan *recvTask, 100),
	    streamInfos: NewStreamInfos(),
	    getShadow: getShadow,
	    getShadowIp: getShadowIp,
    }
	handler.activeStreams = newActiveStreamStore(ctx, datastore, node, handler.activeWorkers.newFileAdd)
	handler.service = service.NewService(account, handler, node)
	return handler
}


func (h *StreamService) GetParent(sid string) string {
   return h.streamInfos.getParent(sid)
}

func (h *StreamService) GetStatus(sid string) (pb.StreamStatus, bool) {
	return h.streamInfos.getStatus(sid)
}

// Protocol returns the handler protocol
func (h *StreamService) Protocol() protocol.ID {
	return streamServiceProtocol
}

// Start begins online services
func (h *StreamService) Start() {
	maxWorkers = defaultMaxWorkers
	go h.handleRecvTask()
    h.online = true
	h.service.Start()
    // TODO:
    // 		It may not be a good idea to use StreamService as StreamNotifee directly.
	h.service.Node().PeerHost.Network().Notify((*StreamNotifee)(h))

    // Run periodic jobs such as clean stream infos
    go func() {
        h.runJobs()
    }()
}

func (h *StreamService) CreateTCPConnPool(){
	//if h.cp==nil || h.cp.closed {
	//	socketAddr:=h.getShadowIp()+":40121"
	//	log.Debugf("create tcp pool: %s",socketAddr)
	//	h.cp,_=NewConnPool(func()(ConnEle,error){return net.Dial("tcp",socketAddr)},30,time.Second*10)
	//}else{
	//	log.Debugf("tcp pool already created")
	//}
}

func (h *StreamService) runJobs() {
    freq := time.Minute * 30
	tick := time.NewTicker(freq)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
            h.streamInfos.clearObsoleteInfos()
		case <-h.ctx.Done():
			return
		}
	}
}

// Ping pings another peer
func (h *StreamService) Ping(pid peer.ID) (service.PeerStatus, error) {
	return h.service.Ping(pid.Pretty())
}

// Handle is called by the underlying service handler method
func (h *StreamService) Handle(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	log.Debugf("[%s] %s", TAG_STREAM_STARTHANDLE, env.Message.Type.String())
	recorder.Hlog.Add(fmt.Sprintf("[%s] %s", TAG_STREAM_STARTHANDLE, env.Message.Type.String()))
	defer func () {
		log.Debug("[%s] %s", TAG_STREAM_DONEHANDLE, env.Message.Type.String())
		recorder.Hlog.Add(fmt.Sprintf("[%s] %s", TAG_STREAM_DONEHANDLE, env.Message.Type.String()))
	}()
	switch env.Message.Type {
	case pb.Message_STREAM_BLOCK:
		return h.handleStreamBlock(env, pid)
	case pb.Message_STREAM_BLOCK_LIST:
		return h.handleStreamBlockList(env, pid)
	case pb.Message_STREAM_REQUEST:
		return h.handleStreamRequest(env, pid)
	case pb.Message_STREAM_UNSUBSCRIBE:
		return h.handleUnsubscribe(env, pid)
	case pb.Message_STREAM_PUSH_INFORM:
		return h.handleStreamPushInform(env, pid)
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
	//selfPeerId := h.service.Node().Identity.Pretty()
	//acceptedSubstream := newProvidedSubstream(config.Id, 1, 1, 0, selfPeerId, h.handleBlockLost)
	//provider := h.providers.getOrCreate(selfPeerId)
	//provider.add(acceptedSubstream)
	info := h.streamInfos.getOrCreate(config.Id)
	info.onCreateStream()
}

/*
 * Start a new stream, where the stream id is exactly the file cid
 */
func (h *StreamService) FileAsStream(sf *pb.StreamFile, fileType pb.StreamMeta_Type) (*pb.StreamMeta, error){
	meta, err := h.activeStreams.fileAsStream(sf, fileType)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	info := h.streamInfos.getOrCreate(meta.Id)
	info.onCreateStream()
    return meta, nil
}

func (h *StreamService) SetMaxWorkers(n int) {
	log.Debugf("Change max workers to %d", n)
	recorder.Hlog.Add(fmt.Sprintf("Change max workers to %d", n))
	maxWorkers = n
}

func (h *StreamService) GetMaxWorkers() int {
	return maxWorkers
}

func (h *StreamService) SetStreamMode(n int) {
	log.Debugf("Change stream mode to %d", n)
	recorder.Hlog.Add(fmt.Sprintf("Change stream mode to %d", n))
	h.mode = StreamMode(n)
}

func (h *StreamService) GetStreamMode() StreamMode {
	return h.mode
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
}

// UnsubscribeStream want to unsubscribe to a stream, and send a request to the
// provider.
func (h *StreamService) UnsubscribeStream(sid string) error{
    fmt.Printf("StreamService: Try to unsubscribe stream %s\n", sid)
    rStream := h.providedStreams.remove(sid)
    if rStream != nil {
    	_, err := h.SendUnsubscribeRequest(rStream.providerId, sid)
    	return err
	}
	return nil
}

/*
 * ThreadGetStream is called when a new stream meta comes through application (thread most case).
 * In that case, a timer will be set.
 * If no inform receive before the timer end, the status would be set to timeout.
 */
func (h *StreamService) OnStreamMeta(meta *pb.StreamMeta, treePrevious []string) {
	//timer := time.NewTicker()
	honlog.Hlog.Add("[OnStreamMeta] " + meta.Type.String())
	info := h.streamInfos.getOrCreate(meta.Id)
	info.onMeta(func(){
		info.onInformTimeout()
		request := &pb.StreamRequest{
			Id:                   meta.Id,
			StreamMap:            1,
			StartIndex:           0,
		}
		for _, pid := range(treePrevious) {
			responseEnv, err := h.SendStreamRequest(pid, request)
			if err != nil {
				log.Errorf("Error when send request %s to %s", meta.Id, pid)
				recorder.Hlog.Add("Error when send request "+meta.Id+" to "+pid)
				continue
			}
			response := new(pb.StreamRequestHandle)
			err = ptypes.UnmarshalAny(responseEnv.Message.Payload, response)
			if err!=nil {
				log.Error("Fail to unmarshal inform request response.", err)
				continue
			}
			if response.Value != 1 {
				log.Debugf("Request %s denied by %s", meta.Id, pid)
				recorder.Hlog.Add("Request "+ meta.Id +" denied by "+pid)
				continue
			} else {
				info, ok := h.streamInfos.get(meta.Id)
				if !ok {
					recorder.Hlog.Add("Stream info for strem "+ meta.Id +" not exists")
					break
				}
				info.onRequestSuccess(func() {
					info.sLock.Lock()
					info.status = pb.StreamStatus_RECEIVE_TIMEOUT
					info.sLock.Unlock()
					h.SendUnsubscribeRequest(pid, meta.Id)
					pdate, _ := ptypes.TimestampProto(time.Now())
					note := &pb.Notification{
						Id:          ksuid.New().String(),
						Date:        pdate,
						//Actor:       pid.Pretty(),
						Subject:     meta.Id,
						Target:      "",
						Type:        pb.Notification_INFORM_TIMEOUT,
					}

					err := h.sendNotification(note)
					if err != nil {
						log.Error("Error when send notification ", err)
						honlog.Hlog.Add("Error when send notification "+err.Error())
					}
				})
			}
		}
		pdate, _ := ptypes.TimestampProto(time.Now())
		note := &pb.Notification{
			Id:          ksuid.New().String(),
			Date:        pdate,
			//Actor:       pid.Pretty(),
			Subject:     meta.Id,
			Target:      "",
			Type:        pb.Notification_INFORM_TIMEOUT,
		}

		err := h.sendNotification(note)
		if err != nil {
			log.Error("Error when send notification ", err)
			honlog.Hlog.Add("Error when send notification "+err.Error())
		}
	})
}

// ======================== FOR MESSAGE RECV/SEND ==================================
// handleStreamBlock receives a STREAM_BLOCK message [deprecated]
func (h *StreamService) handleStreamBlock(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	log.Error("Should not call handleStreamBlock")
	recorder.Hlog.Add("Should not call handleStreamBlock")
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
	recorder.Hlog.Add(fmt.Sprintf("[%s] Block %s, Stream %s, From %s, Size %d", TAG_BLOCKRECEIVE, cid.String(), blk.StreamID, pid.Pretty(), stat.Size()))
	log.Debugf("[%s] Block %s, Stream %s, From %s, Size %d", TAG_BLOCKRECEIVE, cid.String(), blk.StreamID, pid.Pretty(), stat.Size())
    err = h.datastore.StreamBlocks().Add(model)
    return nil, err
}
type recvTask struct {
	env *pb.Envelope
	pid peer.ID
}

func (h *StreamService) handleRecvTask(){
	for {
		select {
			case task, ok := <-h.recvBuf:
				if !ok {
					log.Error("Read from recvBuf failed.")
					honlog.Hlog.Add("Read from recvBuf failed.")
					continue
				}
				env := task.env
				pid := task.pid
				streams := make(map[string]int)
				blks := new(pb.StreamBlockContentList)
				err := ptypes.UnmarshalAny(env.Message.Payload, blks)
				if err != nil {
					log.Error("Error when unmarshal payload from env: ", err)
					honlog.Hlog.Add("Error when unmarshal payload from env: " + err.Error())
					continue
				}


				for _, blk := range blks.Blocks {

					size := len(blk.Data)
					cidStr := ""
					if size != 0 {
						stat, err := ipfs.PutBlock(h.service.Node(), bytes.NewReader(blk.Data))
						if err != nil {
							//return nil, err
							log.Error("Error when put block to ipfs: ", err)
							honlog.Hlog.Add("Error when put block to ipfs: " + err.Error())
							continue
						}
						cid := stat.Path().Cid()
						cidStr = cid.String()
					}
					model := &pb.StreamBlock{
						Id:          cidStr,
						Streamid:    blk.StreamID,
						Index:       blk.Index,
						Size:        int32(size),
						IsRoot:      blk.IsRoot,
						Description: string(blk.Description),
					}
					//fmt.Printf("StreamService: Received stream %s; index %d; cid %s\n", blk.StreamID, blk.Index, cid.String())
					recorder.Hlog.Add(fmt.Sprintf("[%s] Block %s, Stream %s, Index %d, From %s, Size %d, IsRoot %v", TAG_BLOCKRECEIVE, cidStr, blk.StreamID, blk.Index, pid.Pretty(), size, model.IsRoot))
					log.Debugf("[%s] Block %s, Stream %s, Index %d, From %s, Size %d, IsRoot %v", TAG_BLOCKRECEIVE, cidStr, blk.StreamID, blk.Index, pid.Pretty(), size, model.IsRoot)
					err = h.datastore.StreamBlocks().Add(model)
					if err != nil {
						log.Error("Error when store block to datastore: ", err)
						honlog.Hlog.Add("Error when store block to datastore: " + err.Error())
					}

					/*
					 * TODO:
					 *		There is still a bug when block is received before meta.
					 *		In that case, getOrCreate() would create a providedStream with startIndex 0.
					 *		It is ok if the startIndex IS EXACTLY 0.
					 *		Otherwise providedStream may think there is blocks unreceived before root block.
					 *		See implementation of providedStream.addBlock() for details.
					 */
					pStream := h.providedStreams.getOrCreate(blk.StreamID, pid.Pretty(), 0)
					rootBlocks := pStream.addBlock(model)
					for _, b := range rootBlocks {
						err = h.handleRootBlk(pid, b)
						if err != nil {
							log.Errorf("Error when handle rootblock: %v", err)
							recorder.Hlog.Add(fmt.Sprintf("Error when handle rootblock: %v", err))
						}
					}
					streams[blk.StreamID] = 1
				}
				for id := range streams {
					err := h.activeWorkers.newFileAdd(id)
					if err != nil {
						log.Error(err)
					}
                    h.refreshProviderTimer(id)
				}
			case <-h.ctx.Done():
				err := h.ctx.Err()
				log.Error("Stream context end: ", err)
				honlog.Hlog.Add("Stream context end: " + err.Error())
				return
		}

	}
}

func (h *StreamService) refreshProviderTimer(sid string) {
    info, ok := h.streamInfos.get(sid)
    if ok{
        info.refreshProviderTimer()
    }
}


// handleStreamBlock receives a STREAM_BLOCK_LIST message
func (h *StreamService) handleStreamBlockList(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	h.recvBuf <- &recvTask{
		env: env,
		pid: pid,
	}
	//fmt.Printf("StreamService: New stream blk list receive from %s\n", pid.Pretty())
	/*
	streams := make(map[string]int)
	blks := new(pb.StreamBlockContentList)
	err := ptypes.UnmarshalAny(env.Message.Payload, blks)
	if err != nil {
		return nil, err
	}

	for _, blk := range blks.Blocks {
		size := len(blk.Data)
		cidStr := ""
		if size != 0 {
			stat, err := ipfs.PutBlock(h.service.Node(), bytes.NewReader(blk.Data))
			if err != nil {
				return nil, err
			}
			cid := stat.Path().Cid()
			cidStr = cid.String()
		}
		model := &pb.StreamBlock{
			Id:          cidStr,
			Streamid:    blk.StreamID,
			Index:       blk.Index,
			Size:        int32(size),
			IsRoot:      blk.IsRoot,
			Description: string(blk.Description),
		}
		//fmt.Printf("StreamService: Received stream %s; index %d; cid %s\n", blk.StreamID, blk.Index, cid.String())
		recorder.Hlog.Add(fmt.Sprintf("[%s] Block %s, Stream %s, Index %d, From %s, Size %d, IsRoot %v", TAG_BLOCKRECEIVE, cidStr, blk.StreamID, blk.Index, pid.Pretty(), size, model.IsRoot))
		log.Debugf("[%s] Block %s, Stream %s, Index %d, From %s, Size %d, IsRoot %v", TAG_BLOCKRECEIVE, cidStr, blk.StreamID, blk.Index, pid.Pretty(), size, model.IsRoot)
		err = h.datastore.StreamBlocks().Add(model)
		if err != nil {
			return nil, err
		}


		 * TODO:
		 *		There is still a bug when block is received before meta.
		 *		In that case, getOrCreate() would create a providedStream with startIndex 0.
		 *		It is ok if the startIndex IS EXACTLY 0.
		 *		Otherwise providedStream may think there is blocks unreceived before root block.
		 *		See implementation of providedStream.addBlock() for details.
		pStream := h.providedStreams.getOrCreate(blk.StreamID, pid.Pretty(), 0)
		rootBlocks := pStream.addBlock(model)
		for _, b := range rootBlocks {
			err = h.handleRootBlk(pid, b)
			if err != nil {
				log.Errorf("Error when handle rootblock: %v", err)
				recorder.Hlog.Add(fmt.Sprintf("Error when handle rootblock: %v", err))
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
	*/
    return nil, nil
}

// handleRootBlk does following works
//		- Send Notification to application
//		- Update number of blocks in streammeta datastore
func (h *StreamService) handleRootBlk(pid peer.ID, blk *pb.StreamBlock) error {
	log.Debug("Handle root block ", blk.Id)
	recorder.Hlog.Add("Handle root block"+blk.Id)
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

    if blk.Id == "" {
    	log.Debugf("[%s] Stream %s", TAG_STREAM_COMPLETE, blk.Streamid)
    	recorder.Hlog.Add(fmt.Sprintf("[%s] Stream %s", TAG_STREAM_COMPLETE, blk.Streamid))
        meta := h.datastore.StreamMetas().Get(blk.Streamid)
	    if meta == nil || meta.Nblocks > 0{
			log.Errorf("No stream meta for stream %s root block %s", blk.Streamid, blk.Id)
		    return nil
	    }
        err := h.datastore.StreamMetas().UpdateNblocks(blk.Streamid, blk.Index + 1)
        if err != nil {
            log.Error(err)
            return err
        }
        // Remove provider here
		// h.providers.RemoveStream(blk.Streamid)
		endStream := h.providedStreams.remove(blk.Streamid)
		if endStream == nil {
			log.Error("No providedStream ", blk.Streamid)
			honlog.Hlog.Add("No providedStream" + blk.Streamid)
		} else {
			recvDuration := time.Since(endStream.startTime).Milliseconds()
			h.streamInfos.setDuration(blk.Streamid, recvDuration)
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
		log.Error("Error when send notification to application: ", err)
		recorder.Hlog.Add("Error when send notification to application: " + err.Error())
		return err
	}

    return nil
}

func (h* StreamService) GetDuration(streamId string) int64 {
	return h.streamInfos.getDuration(streamId)
}

func (h* StreamService) handleStreamPushInform(env *pb.Envelope, peer peer.ID) (*pb.Envelope, error) {
	log.Debugf("Receive streamPushInform from ", peer.Pretty())
	recorder.Hlog.Add("Receive streamPushInform from " + peer.Pretty())
	inform := new(pb.StreamPushInform)
	err := ptypes.UnmarshalAny(env.Message.Payload, inform)
	if err != nil {
		log.Error("Fail to unmarshal inform from envelop.")
		return nil, err
	}

	// ========= change status
	info := h.streamInfos.getOrCreate(inform.Meta.Id)
	canRequest := info.onInform()
	if !canRequest {
		return nil,nil
	}

	// =========

	meta := inform.Meta
	localMeta := h.datastore.StreamMetas().Get(meta.Id)
	last := h.datastore.StreamBlocks().LastIndex(meta.Id)
	if localMeta != nil && last == localMeta.Nblocks && last != 0{
		info.status = pb.StreamStatus_COMPLETE
		err = h.informForward(inform)
		log.Debugf("Can not sending request: ", meta.Id)
		honlog.Hlog.Add("Can not sending request: " + meta.Id)
		return nil, err
	}

	request := &pb.StreamRequest{
		Id:                   meta.Id,
		StreamMap:            1,
		StartIndex:           last,
	}
	responseEnv, err := h.SendStreamRequest(peer.Pretty(), request)
	if err != nil {
		log.Errorf("Error when send request %s to %s", meta.Id, peer.Pretty())
		recorder.Hlog.Add("Error when send request "+meta.Id+" to "+peer.Pretty())
		return nil, err
	}
	response := new(pb.StreamRequestHandle)
	err = ptypes.UnmarshalAny(responseEnv.Message.Payload, response)
	if err!=nil {
		log.Error("Fail to unmarshal inform request response.", err)
		return nil, err
	}
	if response.Value != 1 {
		log.Debugf("Request %s denied by %s", meta.Id, peer.Pretty())
		recorder.Hlog.Add("Request "+ meta.Id +" denied by "+peer.Pretty())
	} else {
		log.Debugf("Request %s accepted by %s", meta.Id, peer.Pretty())
		h.RequestAccepted(peer.Pretty(), request)
		info, ok := h.streamInfos.get(meta.Id)
		if !ok {
			log.Error("No info when get request response")
			honlog.Hlog.Add("No info when get request response")
		} else {
			info.onRequestSuccess(func() {
				info.sLock.Lock()
				info.status = pb.StreamStatus_RECEIVE_TIMEOUT
				info.sLock.Unlock()
				h.SendUnsubscribeRequest(peer.Pretty(), meta.Id)
				pdate, _ := ptypes.TimestampProto(time.Now())
				note := &pb.Notification{
					Id:          ksuid.New().String(),
					Date:        pdate,
					//Actor:       pid.Pretty(),
					Subject:     meta.Id,
					Target:      "",
					Type:        pb.Notification_INFORM_TIMEOUT,
				}

				err := h.sendNotification(note)
				if err != nil {
					log.Error("Error when send notification ", err)
					honlog.Hlog.Add("Error when send notification "+err.Error())
				}
			})
		}
		// =========== Add Meta to DB ===============
		err = h.datastore.StreamMetas().Add(meta)
		if err != nil {
			log.Error("Error when add inform meta to db: ", err)
			recorder.Hlog.Add("Error when add inform meta to db: "+err.Error())
		}
		// =========== Forward Inform to Other Peer ===========
		err = h.informForward(inform)
		if err != nil {
			log.Error("Error when forward inform: ", err)
			recorder.Hlog.Add("Error when forward inform: "+err.Error())
		}

	}
	return nil, nil
}


// HandleStream is called by the underlying service handler method
func (h *StreamService) HandleStream(_ *pb.Envelope, _ peer.ID) (chan *pb.Envelope, chan error, chan interface{}) {
	return make(chan *pb.Envelope), make(chan error), make(chan interface{})
}

// SendMessage sends a message to a peer.
func (h *StreamService) SendMessage(ctx context.Context, peerId string, env *pb.Envelope) error {
	return h.service.SendMessage(ctx, peerId, env)
}

func (h *StreamService) IsBusy() bool {
	numWorkers := h.Workload()
	return numWorkers >= maxWorkers
	//return false
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
    //if numWorkers < maxWorkers {
    	log.Debugf("[%s], Stream %s, To %s, Workers %d", TAG_STREAMREQUESTACCEPT, req.Id, pid.Pretty(), numWorkers)
    	recorder.Hlog.Add(fmt.Sprintf("[%s], Stream %s, To %s, Workers %d", TAG_STREAMREQUESTACCEPT, req.Id, pid.Pretty(), numWorkers))
        err = h.responseRequest(pid, req)
        if err != nil {
            return nil, err
        }
        return h.service.NewEnvelope(pb.Message_STREAM_REQUEST_HANDLE, &pb.StreamRequestHandle{
    	    Value:1,
        },nil, true)
    //} else {
	//	log.Debugf("[%s], Stream %s, To %s, Workers %d", TAG_STREAMREQUESTREJECT, req.Id, pid.Pretty(), numWorkers)
	//	recorder.Hlog.Add(fmt.Sprintf("[%s], Stream %s, To %s, Workers %d", TAG_STREAMREQUESTREJECT, req.Id, pid.Pretty(), numWorkers))
    //    return h.service.NewEnvelope(pb.Message_STREAM_REQUEST_HANDLE, &pb.StreamRequestHandle{
    //	    Value:0,
    //    },nil, true)
    //}
}

func (h *StreamService) SendStreamRequest(peerId string, config *pb.StreamRequest) (*pb.Envelope, error) {
	//fmt.Printf("core/stream_service.go SendStreamRequest to %s\n", peerId)
	env, err := h.service.NewEnvelope(pb.Message_STREAM_REQUEST, config, nil, false)
	if err != nil {
		return nil,err
	}
	log.Debugf("[%s] Stream %s, To %s", TAG_STREAMREQUEST, config.Id, peerId)
	recorder.Hlog.Add(fmt.Sprintf("[%s] Stream %s, To %s", TAG_STREAMREQUEST, config.Id, peerId))
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
    
    h.activeWorkers.endWorker(req.Id, pid.Pretty())
    return nil, nil
}

// RequestAccepted is called when a stream request is accepted by some peer.
func (h *StreamService) RequestAccepted(peerId string, config *pb.StreamRequest){
	log.Debugf("[%s] Stream %s, By %s", TAG_STREAM_REQUEST_ACCEPTED, config.Id, peerId)
	recorder.Hlog.Add(fmt.Sprintf("[%s] Stream %s, By %s", TAG_STREAM_REQUEST_ACCEPTED, config.Id, peerId))
	//acceptedSubstream := newProvidedSubstream(config.Id, config.StreamMap, 1, config.StartIndex, peerId, h.handleBlockLost)
    //h.treeParent[config.Id] = peerId
	
    h.streamInfos.setParent(config.Id, peerId)
	h.providedStreams.getOrCreate(config.Id, peerId, config.StartIndex)
}


// Call it when you decide to send blocks to requestor.
// Use "Response" to distinguish with "Handle".
func (h *StreamService) responseRequest(pid peer.ID, req *pb.StreamRequest) error {
	log.Debugf("[%s] Stream %s, From %s", TAG_STREAMRESPONSE, req.Id, pid.Pretty())
	recorder.Hlog.Add(fmt.Sprintf("[%s] Stream %s, From %s", TAG_STREAMRESPONSE, req.Id, pid.Pretty()))
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

func (h *StreamService) SendStreamBlcoksToShadow_TCP(peerId peer.ID, blks []*pb.StreamBlock) error{
	log.Debugf("use tcp to send blocks to %s",peerId.String())
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
		blist.Blocks = append(blist.Blocks, content)
	}

	//tcp socket from h.getShadow(), send the blist pb
	//conn1,_:=h.cp.Get()
	//blistData,_:=proto.Marshal(blist)
	//conn1.(net.Conn).Write([]byte(string(len(blistData))))
	//conn1.(net.Conn).Write(blistData)
	//conn1.(net.Conn).Write([]byte("tcpend"))
	//h.cp.Put(conn1)

	conn, _ := net.Dial("tcp", h.getShadowIp()+":40121")

	blistData,_:=proto.Marshal(blist)
	conn.Write(blistData)

	conn.Close()
	return nil
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
        recorder.Hlog.Add(fmt.Sprintf("[%s] Isroot %t, Block %s, Stream %s, Index %d, To %s, Size %d, description: %s", TAG_BLOCKSEND, blk.IsRoot, blk.Id, blk.Streamid, blk.Index, peerId.Pretty(), blk.Size, blk.Description))
        blist.Blocks = append(blist.Blocks, content)
    }
	env, err := h.service.NewEnvelope(pb.Message_STREAM_BLOCK_LIST, blist, nil, false)
	if err != nil {
        log.Error(err)
		return err
	}
	// Send envelope use StreamService.service.SendMessage
    err = h.service.SendMessage(nil, peerId.Pretty(), env)

    // TODO: Remove this after test!!!
    time.Sleep(100 * time.Millisecond)
    // =========================

	var ind1, ind2 uint64
	var streamId1 string
	if len(blks) > 0 {
		ind1 = blks[0].Index
		ind2 = blks[len(blks)-1].Index
		streamId1 = blks[0].Streamid
	}
    if err != nil {
		log.Debugf("[%s] Stream %s, Index %v - %v, To %s", TAG_BLOCKSEND_FAILED, streamId1, ind1, ind2, peerId.Pretty())
    	recorder.Hlog.Add(fmt.Sprintf("[%s] Stream %s, Index %v - %v, To %s", TAG_BLOCKSEND_FAILED, streamId1, ind1, ind2, peerId.Pretty()))
        log.Error(err)
    	return err
    }
	log.Debugf("[%s] Stream %s, Index %v - %v, To %s", TAG_BLOCKSEND_COMPLETE, streamId1, ind1, ind2, peerId.Pretty())
	recorder.Hlog.Add(fmt.Sprintf("[%s] Stream %s, Index %v - %v, To %s", TAG_BLOCKSEND_COMPLETE, streamId1, ind1, ind2, peerId.Pretty()))
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

// =============== FOR WORKDERS ==================
func (h *StreamService) createWorker(pid peer.ID, req *pb.StreamRequest) (*streamWorker, error) {
	//fmt.Printf("stream/streamManager createWorker")
    stream := h.datastore.StreamMetas().Get(req.Id)
	if stream == nil {
		return nil, ErrUnknowkStream
	}
	if pid.String() == h.getShadow() { // if the requester is shadow, then use TCP
		log.Debugf("the requester is shadow, will use TCP to send blocks")
		//return newStreamWorker(h.ctx, stream, pid, req, h.FetchBlocks, h.SendStreamBlcoksToShadow_TCP, h.taskQueue,true), nil
		return newStreamWorker(h.ctx, stream, pid, req, h.FetchBlocks, h.SendStreamBlocks, h.taskQueue,false), nil
	}else{ // normal peer
		return newStreamWorker(h.ctx, stream, pid, req, h.FetchBlocks, h.SendStreamBlocks, h.taskQueue,false), nil
	}
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
	log.Debugf("Stream service: Peer %s disconnected", pid)
	h.activeWorkers.endPeer(pid.Pretty())
    pStreams := h.providedStreams.providedBy(pid.Pretty())
    for _, s := range pStreams {
    	err := h.subscribe(s.streamId)
    	if err != nil {
    		log.Error(err)
		}
	}
}

func (h* StreamService) GetProvidedHopcnt(config *pb.StreamRequest) (int, bool){
	return 0, false
}

// TODO:
//		Use a different function to check activestreams
func (h* StreamService) GetProvider(sid string) peer.ID{
	if h.activeStreams.isActive(sid) {
		return h.service.Node().Identity
	}
	return peer.ID(h.providedStreams.getProvider(sid))
}
// ===================== OTHERS =========================
func (h *StreamService) Loggable() map[string]interface{}{
	return h.activeWorkers.Loggable()
}

// ====================== For Push ======================
func (h *StreamService) InformPush(peerId string, streamMeta *pb.StreamMeta, tree map[string][]string) error {
	log.Debug("Inform push ", streamMeta.Id, " to ", peerId)
	recorder.Hlog.Add("Inform push " + streamMeta.Id + " to " + peerId)
	treeData, err := json.Marshal(tree)
	if err != nil {
		log.Error("Fail to marshal tree to bytes.")
		recorder.Hlog.Add("Error: Fail to marshal tree to bytes.")
		return errors.New("fail to marshal tree to bytes")
	}

	// build envelope
	inform := &pb.StreamPushInform{
		Meta: streamMeta,
		Tree:                 treeData,
	}
	env, err := h.service.NewEnvelope(pb.Message_STREAM_PUSH_INFORM, inform, nil, false)
	if err != nil {
		log.Error(err)
		return err
	}
	// Send envelope use StreamService.service.SendMessage
	err = h.service.SendMessage(nil, peerId, env)
	if err != nil {
		log.Error("Failed to send stream inform to ", peerId)
		recorder.Hlog.Add("Failed to send stream inform to " + peerId)
		return err
	}
	return nil
}

// Forward the inform to other peers when receive an inform
func (h *StreamService) informForward(inform *pb.StreamPushInform) error {
	// unmarshal tree
	tree := make(map[string][]string)
	err := json.Unmarshal(inform.Tree, &tree)
	if err != nil {
		log.Error("Error when unmarshal tree from inform. ", err)
		recorder.Hlog.Add("Error when unmarshal tree from inform. " + err.Error())
		return err
	}

	toPeers := tree[h.service.Node().Identity.Pretty()]
	for _, pid := range toPeers{
		log.Debug(pid)
		env, err := h.service.NewEnvelope(pb.Message_STREAM_PUSH_INFORM, inform, nil, false)
		if err != nil {
			log.Error(err)
			return err
		}
		// Send envelope use StreamService.service.SendMessage
		err = h.service.SendMessage(nil, pid, env)
		if err != nil {
			log.Error("Failed to send stream inform to ", pid)
			recorder.Hlog.Add("Failed to send stream inform to " + pid)
			return err
		}
	}
	return nil
}

