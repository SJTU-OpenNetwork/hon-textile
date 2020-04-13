package core

import (
	"fmt"
	"time"
	//	"github.com/SJTU-OpenNetwork/hon-textile/stream"
	"github.com/ipfs/go-cid"

	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/golang/protobuf/proto"
	//stream "github.com/SJTU-OpenNetwork/go-stream"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
)
var ErrStreamNotFound = fmt.Errorf("stream not found")
var ErrStreamAlreadyInUse = fmt.Errorf("stream already in use")
type ErrStreamAlreadyExist struct {
	meta *pb.StreamMeta
}
func (e *ErrStreamAlreadyExist) Error() string {
	return fmt.Sprintf("Stream %s already exist in datastore.", e.meta.Id)
}
type ErrStreamNotExist struct {
	Id string
}
func (e *ErrStreamNotExist) Error() string {
	return fmt.Sprintf("Stream %s not exist in datastore.", e.Id)
}

// StartStream does two tasks:
//	- Add stream to datastore.
//	- Start stream routine.
// TODO:
//		Support renewal for existing stream.
func (t *Textile) StartStream(threadId string, config *pb.StreamMeta) error {
	defer fmt.Printf("textile.StartStream end success\n")
	fmt.Printf("textile.StartStream\n")

	// Check whether this stream already exists in datastore.
	stream := t.GetStreamMeta(config.Id)
	if stream != nil {
		return &ErrStreamAlreadyExist{meta:config}
	}

    err := t.datastore.StreamMetas().Add(config);if err != nil {return err}
    //stream := t.GetStreamMeta(config.Id)
	//if stream == nil {
	//	return ErrStreamNotFound
	//}
    t.stream.StartStream(config)
	//publish the Stream to others
	fmt.Printf("Find thread for stream.\n")
	thread := t.Thread(threadId)
	if thread == nil {
		return ErrStreamNotFound
	}

	//fmt.Printf("Add streamMeta to thread.\n")
	_, err = thread.AddStreamMeta(config)

    if !t.config.IsShadow {
        t.shadow.PushStreamMeta(config, true)
    }

	return err
}

func (t *Textile) CloseStream(threadId string, streamId string) error {
	defer fmt.Printf("textile.CloseStream end success\n")
	fmt.Printf("textile.CloseStream\n")
	
    stream := t.GetStreamMeta(streamId)
	if stream == nil {
        return &ErrStreamNotExist{Id: streamId}
	}

	t.stream.CloseStream(streamId)

    //TODO: update nblocks, how to get the total number of blocks?
    //TODO: send stream close message through thread
    return nil
}

/* [deprecated] use stream_meta instead
func (t *Textile) GetStream(id string) *pb.Stream {
	return t.datastore.Streams().Get(id)
}
 */

func (t *Textile) GetStreamMeta(id string) *pb.StreamMeta {
	return t.datastore.StreamMetas().Get(id)
}

func (t *Textile) ListStreamMeta() *pb.StreamMetaList{
	return t.datastore.StreamMetas().List()
}

// add the new file to the corresponding channel
func (t *Textile) StreamAddFile(id string, sf *pb.StreamFile) error {
    t.stream.StreamAddFile(id, sf)
    //t.stream.sm.StreamAddFile(id, sf)
	return nil
}

func (t *Textile) handleProviderSearchResult(resultCh <-chan *pb.QueryResult, errCh <-chan error, cancel *broadcast.Broadcaster, config *pb.StreamRequest) (error) {
	log.Debugf("in handleProviderSearchResult")
    timer := time.NewTimer(time.Second * 2) //Wait for 2s
    doneCh := make(chan struct{}, 1)
	done := func() {
		// Use select to avoid block when there is already done signal in channel.
		select {
			case doneCh <- struct{}{}:
			default:
		}
    }
    go func() {
		<-timer.C
        log.Debug("Search time out")
		done()
	}()
	go func() {
		for {
			select {
			case <-doneCh:
				log.Debugf("result channel done")
                t.SubscribeNotify(config.Id, false)
                close(doneCh)
				return
			case err := <-errCh:
                log.Error(err)
                t.SubscribeNotify(config.Id, false)
				return

			case res, ok := <-resultCh:
                log.Debug("get result!")
				if !ok {
                    log.Debug("Error occur")
					return
				}
				item := &pb.StreamQueryResultItem {}
				proto.Unmarshal(res.Value.Value, item)

                //if already have provider
                //just break
                if t.stream.GetProvider(config.Id) != peer.ID("") {
                    log.Debug("provider alread exists")
                    done()
                    timer.Stop()
                    break
                }

                // if the provider is connected, request the stream directly
                log.Debug("PID: "+item.Pid)
                connected, err := ipfs.SwarmConnected(t.node, item.Pid) 
	            if err != nil{
                    log.Error(err)
	    	        break
                }
                
                if connected {
                    log.Debug("Try request stream")
                    env, err := t.RequestStream(item.Pid, config)
	                if err != nil{
                        log.Error(err)
	    	            break
                    }
                    response := new(pb.StreamRequestHandle)
                    err = ptypes.UnmarshalAny(env.Message.Payload, response)
	                if err!=nil {
                        log.Error(err)
	    	            break
	                }
                    if response.Value != 1 {
                        log.Errorf("request %s failed", item.Pid)
	    	            break
                    } else {
                        t.SubscribeNotify(config.Id, true)
                        // request is accepted, kill the handle process
                        return
                    }
                } else {
                    log.Debug("Not connected, exitting")
                }
			}
		}
	}()
	return nil
}

// do not support substream currently
func (t *Textile) SubscribeNotify(id string, res bool) {
    if res {
        log.Debugf("Subscribe stream "+id+" success")
    } else {
        log.Debugf("Subscribe stream "+id+" fail")
    }
}

func (t* Textile) SubscribeStream(id string) error {
    if t.stream.GetProvider(id) != peer.ID("") {
        return fmt.Errorf("Resubscribe stream "+id)
    }
   
    last := t.datastore.StreamBlocks().LastIndex(id)

    config := &pb.StreamRequest {
        Id: id,
        StreamMap: 1,
        StartIndex: last,
    }

    // shadow node should subscribe the same stream
    meta := t.GetStreamMeta(id)
	if meta != nil && !t.config.IsShadow {
        t.shadow.PushStreamMeta(meta, false)
	}

	// call search stream
    query := & pb.StreamQuery { 
        Id: id,
    }
    opt := &pb.QueryOptions {
        Wait: 2,
        Limit: 10,
    }
    resCh, errCh, cancel, err := t.SearchStream(query, opt)
    if err != nil {
        return err
    }
    t.handleProviderSearchResult(resCh, errCh, cancel, config)
	return nil
}

func (t* Textile) UnsubscribeStream(id string) error{
    err := t.stream.UnsubscribeStream(id)
    return err
}

func (t* Textile) RequestStream(pid string, config *pb.StreamRequest) (*pb.Envelope, error){
	return t.stream.SendStreamRequest(pid, config)
}

func (t* Textile) StreamRequestAccepted(pid string, config *pb.StreamRequest) {
	t.stream.RequestAccepted(pid, config)
}

func (t *Textile) SearchStream(query *pb.StreamQuery, options *pb.QueryOptions) (<-chan *pb.QueryResult, <-chan error, *broadcast.Broadcaster, error) {
	log.Debug("in SearchStream")
	payload, err := proto.Marshal(query)
	if err != nil {
		return nil, nil, nil, err
	}

	options.Filter = pb.QueryOptions_HIDE_OLDER

	resCh, errCh, cancel := t.searchByPubsub(&pb.Query{
	//resCh, errCh, cancel := t.searchAll(&pb.Query{
		Type:    pb.Query_STREAM,
		Options: options,
		Payload: &any.Any{
			TypeUrl: "/StreamQuery",
			Value:   payload,
		},
	})
	return resCh, errCh, cancel, nil
}


func (t *Textile) GetStreamBlocks(streamId string, startIndex int) ([]cid.Cid, error) {
	// Fetch the blocks from startIndex
	return nil, nil
}

func (t *Textile) StreamWorkerStat() {
	t.stream.WorkerStat()
}

