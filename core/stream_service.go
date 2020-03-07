// Service for sending/receving stream related data - add by Jerry 2020/02/25

package core

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/interface-go-ipfs-core/path"
	"io/ioutil"

    "bytes"
	"context"
//	"encoding/base64"
//	"fmt"
	"time"
    "github.com/segmentio/ksuid"

//"github.com/golang/protobuf/proto"
//    "github.com/ipfs/go-cid"
	"github.com/golang/protobuf/ptypes"
	"github.com/SJTU-OpenNetwork/go-ipfs/core"
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
	"github.com/SJTU-OpenNetwork/hon-textile/stream"
//    stream "github.com/SJTU-OpenNetwork/go-stream"
)


// streamServiceProtocol is the current protocol tag
const streamServiceProtocol = protocol.ID("/textile/stream/1.0.0")

type StreamService struct {
	service          *service.Service
	datastore        repo.Datastore
    sm               *stream.StreamManager
	online           bool
	sendNotification func(*pb.Notification) error
}

// NewStreamService returns a new stream service
func NewStreamService(
	account *keypair.Full,
	node func() *core.IpfsNode,
	datastore repo.Datastore,
	sendNotification func(*pb.Notification) error,
) *StreamService {
	handler := &StreamService{
		datastore:        datastore,
		sendNotification: sendNotification,
	}
	handler.service = service.NewService(account, handler, node)
    handler.sm = stream.NewStreamManager(handler.FetchBlocks, handler.FetchStream, handler.SendStreamBlocks)
	return handler
}

// Protocol returns the handler protocol
func (h *StreamService) Protocol() protocol.ID {
	return streamServiceProtocol
}

// Start begins online services
func (h *StreamService) Start() {
	h.service.Start()
}

// Ping pings another peer
func (h *StreamService) Ping(pid peer.ID) (service.PeerStatus, error) {
	return h.service.Ping(pid.Pretty())
}

// Function for command workerStat.
// Called by Textile directly. Use streamManager to output the stat of all the working workers.
func (h *StreamService) WorkerStat(){
	h.sm.WorkerStat()
}

// handleStreamBlock receives a STREAM_BLOCK message
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

	log.Debugf("[%s] Block %s, Stream %s, From %s, Size %d", stream.TAG_BLOCKRECEIVE, cid.String(), blk.StreamID, pid.Pretty(), stat.Size())
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

        stat, err := ipfs.PutBlock(h.service.Node(), bytes.NewReader(blk.Data))
        if err != nil {
            return nil, err
        }
        cid := stat.Path().Cid()
        model := &pb.StreamBlock {
            Id: cid.String(),
            Streamid: blk.StreamID,
            Index: blk.Index,
            Size: int32(stat.Size()),
            IsRoot: blk.IsRoot,
            Description: string(blk.Description),
        }
        //fmt.Printf("StreamService: Received stream %s; index %d; cid %s\n", blk.StreamID, blk.Index, cid.String())
        log.Debugf("[%s] Block %s, Stream %s, Index %d, From %s, Size %d", stream.TAG_BLOCKRECEIVE, cid.String(), blk.StreamID, blk.Index, pid.Pretty(), stat.Size())
        err = h.datastore.StreamBlocks().Add(model)
        if err != nil {
            return nil, err
        }
        //fmt.Printf("It is successfully stored in our database!\n")

        if blk.IsRoot {
            // we found a file !
            fmt.Print("It is a root node of a merkle-DAG!\n")
            // h.sm.NewBlockReceive(model, []byte(blk.Data))
            // implement root handler in stream_service directly

            //h.sm.NewBlockReceive(model, []byte(blk.Data))
            err = h.handleRootBlk(pid, model)
            if err != nil {
                fmt.Printf("Handle root file failed\n")
                return nil, err
            }
        }
        streams[blk.StreamID] = 1
    }
    for id := range streams {
        h.sm.NewFileAdd(id)
    }
    return nil, nil
}

func (h *StreamService) handleRootBlk(pid peer.ID, blk *pb.StreamBlock) error {
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
		Body:        "stream file",
	}
    err := h.sendNotification(note)
	if err != nil {
		return err
	}
    return nil
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
    default:
    	fmt.Printf("core/stream_service.go Handler: Unknown message type")
        return nil, nil
    }
}

// HandleStream is called by the underlying service handler method
func (h *StreamService) HandleStream(env *pb.Envelope, pid peer.ID) (chan *pb.Envelope, chan error, chan interface{}) {
	return make(chan *pb.Envelope), make(chan error), make(chan interface{})
}

// SendMessage sends a message to a peer
func (h *StreamService) SendMessage(ctx context.Context, peerId string, env *pb.Envelope) error {
	return h.service.SendMessage(ctx, peerId, env)
}

// HandleRequest
func (h *StreamService) handleStreamRequest(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	fmt.Printf("core/stream_service.go handleStreamRequest from %s\n", pid.Pretty())
	req := new(pb.StreamRequest)
	err := ptypes.UnmarshalAny(env.Message.Payload, req)
	if err != nil {
		return nil, err
	}

    if h.sm.Workload() < 5 {
        err = h.sm.ResponseRequest(pid, req)
        if err != nil {
            return nil, err
        }
	    return h.service.NewEnvelope(pb.Message_STREAM_REQUEST_HANDLE, &pb.StreamRequestHandle{
		    Value:1,
	    },nil, true)
    } else {
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
	log.Debugf("[%s] Stream %s, To %s", stream.TAG_STREAMREQUEST, config.Id, peerId)
	return h.service.SendRequest(peerId, env)
}


//SendStreamBlocks send a list of block to a peer
func (h *StreamService) SendStreamBlocks(peerId peer.ID, blks []*pb.StreamBlock) error{
	//fmt.Printf("StreamService: Send %d stream blks to %s\n", len(blks), peerId.Pretty())

	// Marshal blocks to pb
    blist := new(pb.StreamBlockContentList)
    for _, blk:= range blks {
        r, err := ipfs.GetBlock(h.service.Node(), path.New(blk.Id))
        data, err := ioutil.ReadAll(r)
		if err != nil {
            log.Error(err)
			return err
		}
        content := &pb.StreamBlockContent{
            StreamID: blk.Streamid,
            Index: blk.Index,
            Data: data,
            IsRoot: blk.IsRoot,
            Description: []byte(blk.Description),
        }
        log.Debugf("[%s] Block %s, Stream %s, Index %d, To %s, Size %d", stream.TAG_BLOCKSEND, blk.Id, blk.Streamid, blk.Index, peerId.Pretty(), blk.Size)
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
        log.Error(err)
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

// FetchStream fetches a specific stream from dababase
func (h *StreamService)FetchStream(streamId string)(*pb.StreamMeta, error){
    //Get the stream with id = streamId
    stream := h.datastore.StreamMetas().Get(streamId)
    if stream == nil{
    	return nil,fmt.Errorf("stream fetch failed")
	}
    return stream, nil
}
