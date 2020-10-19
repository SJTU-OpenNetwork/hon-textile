package core

import (
	"encoding/json"
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	"github.com/SJTU-OpenNetwork/hon-textile/stream"
	"sort"
	"time"
	//	"github.com/SJTU-OpenNetwork/hon-textile/stream"
	"github.com/ipfs/go-cid"

	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	//stream "github.com/SJTU-OpenNetwork/go-stream"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
)
var ErrStreamNotFound = fmt.Errorf("stream not found")
var ErrStreamAlreadyInUse = fmt.Errorf("stream already in use")
var ErrSubscribeFail = fmt.Errorf("subscribe failed")
var num_retry_map = make(map[string] int)
const MAX_RETRY = 10

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
	if stream == nil {
        err := t.datastore.StreamMetas().Add(config);if err != nil {return err}
        //stream := t.GetStreamMeta(config.Id)
	    //if stream == nil {
	    //	return ErrStreamNotFound
	    //}
        t.stream.StartStream(config)
	} else {
	    log.Warningf("start an old stream")
    }

	//publish the Stream to others
	fmt.Printf("Find thread for stream.\n")
	thread := t.Thread(threadId)
	if thread == nil {
		return ErrThreadNotFound
	}

	//fmt.Printf("Add streamMeta to thread.\n")
    _, err := thread.AddStreamMeta(config)
	if err != nil {
		log.Error(err)
		return err
	}

	//====== send notification to self
	record := &pb.Notification{
		Block:                config.Id,
		Date:                 ptypes.TimestampNow(),
		Actor:                "",	// self id. filled with "" if can not get.
		Subject:              recorder.Event_ThreadAddFile,	// event type
		Target:               "",	// self id. The peer that add the file would be collector
		Read:                 true,	// send to notification channel. There is other notification fot thread add file.
	}
	recorder.RecordCh <- record
	//======

    if !t.config.IsShadow {
        err := t.shadow.PushStreamMeta(config, true)
        if err != nil {
        	log.Error(err)
        	return err
		}
    }
	return nil
}

func (t *Textile) StartStream_Text(threadId string, config *pb.StreamMeta) error {
	defer fmt.Printf("textile.StartStream end success\n")
	fmt.Printf("textile.StartStream\n")

	// Check whether this stream already exists in datastore.
	stream := t.GetStreamMeta(config.Id)
	if stream == nil {
		err := t.datastore.StreamMetas().Add(config);if err != nil {return err}
		t.stream.StartStream(config)
	} else {
		log.Warningf("start an old stream")
	}

	//publish the Stream to others
	fmt.Printf("Find thread for stream.\n")
	thread := t.Thread(threadId)
	if thread == nil {
		return ErrThreadNotFound
	}

	//fmt.Printf("Add streamMeta to thread.\n")
	_, err := thread.AddStreamMeta_Text(config)
	if err != nil {
		log.Error(err)
		return err
	}
	if !t.config.IsShadow {
		err := t.shadow.PushStreamMeta(config, true)
		if err != nil {
			log.Error(err)
			return err
		}
	}
	return nil
}

func (t *Textile) FileAsStream_Text(threadId string, sf *pb.StreamFile, file_type pb.StreamMeta_Type) (*pb.StreamMeta, error) {
	defer fmt.Printf("textile.FileAsStream end success\n")
	fmt.Printf("textile.FileAsStream\n")

	// Check whether this stream already exists in datastore.
	meta, err := t.stream.FileAsStream(sf, file_type)
	if err != nil {
		return nil, err
	}
	streamMeta := t.GetStreamMeta(meta.Id)
	if streamMeta == nil {
		err := t.datastore.StreamMetas().Add(meta);if err != nil {return nil, err}
	} else {
		log.Warningf("start an old stream")
	}

	//publish the Stream to others
	fmt.Printf("Find thread for stream.\n")
	thread := t.Thread(threadId)
	if thread == nil {
		return nil, ErrThreadNotFound
	}

	//fmt.Printf("Add streamMeta to thread.\n")
	_, err = thread.AddStreamMeta_Text(meta)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if !t.config.IsShadow {
		err := t.shadow.PushStreamMeta(meta, true)
		if err != nil {
			log.Error(err)
			return nil, err
		}
	}

	/*
	 * TODO: New mode for stream service
	 * In this mode, peers do not need to search and subscribe stream data unless timeout
	 */
	if t.stream.GetStreamMode() == stream.StreamMode_PUSH {
		workerCnt := t.stream.GetMaxWorkers() 
		streamTree, err := t.constructStreamTree(threadId, workerCnt)
		if err != nil {
			log.Error(err)
			return nil, err
		}

		// Output the tree to log and Hlog
		treeBytes, err := json.MarshalIndent(streamTree, "\n", "  ")
		log.Debug("Push tree: \n", treeBytes)
		recorder.Hlog.Add("Push tree: \n" + string(treeBytes))

		toPeers := streamTree[t.node.Identity.Pretty()]

		for _, pid := range toPeers{
			/* TODO: push stream data (maybe and the streamTree) to peers in toPeers */
			log.Debug(pid)
			err = t.stream.InformPush(pid, meta, streamTree)
			if err != nil {
				log.Error("Fail to push stream inform to ", pid)
				recorder.Hlog.Add("Fail to push stream inform to " + pid)
			}
		}
	}
	return meta, nil
}

/*
 * fetchStreamTreePrevious find some previous previous nodes on the push tree.
 */
func (t *Textile) fetchStreamTreePrevious(threadId string, rootPeer string, number int) ([]string, error) {
	thread := t.Thread(threadId)
	if thread == nil {
		return nil, ErrThreadNotFound
	}

	// Sort all peers
	allPeers := thread.Peers()
	var allPeerIDs []string
	for _, p := range allPeers {
		allPeerIDs = append(allPeerIDs, p.Id)
	}
	sort.Strings(allPeerIDs)

	// Find index of root peer
	var  rootInd int
	for i, p := range allPeerIDs {
		if p == rootPeer {
			rootInd = i
		}
	}

	// Fetch peers:
	//		(number-1) peers behind root peer and the root peer
	threadSize := len(allPeerIDs)
	if threadSize < 2 {
		return nil, nil
	}
	if number > threadSize - 1 {
		number = threadSize - 1
	}
	res := make([]string, 0)
	i:=1
	for len(res)<number-1 {
		tmpPeer := allPeerIDs[(i+rootInd)%threadSize]
		i++
		if tmpPeer != t.node.Identity.Pretty() {
			res = append(res, tmpPeer)
		} else {
			continue
		}
	}
	res = append(res, rootPeer)
	return res, nil
}

func (t *Textile) constructStreamTree(threadId string, workerCnt int) (map[string][]string, error) {
	tree := make (map[string] []string)
	thread := t.Thread(threadId)
	if thread == nil {
		return tree, ErrThreadNotFound
	}
	
	allPeers := thread.Peers()
	var allPeerIDs []string
	for _, p := range allPeers {
		allPeerIDs = append(allPeerIDs, p.Id)
	}
	sort.Strings(allPeerIDs)
	myIndex := 0
	for id, v := range allPeerIDs{
		if v == t.node.Identity.Pretty() {
			myIndex = id
			break
		}
	}
	
	sortedIDs := append(allPeerIDs[myIndex:], allPeerIDs[:myIndex]...)
	for id, v := range sortedIDs{
		sid := id * workerCnt + 1
		if sid >= len(sortedIDs){
			break
		}
		eid := (id+1) * workerCnt + 1
		if eid > len(sortedIDs) {
			eid = len(sortedIDs)
		}
		tree[v] = sortedIDs[sid:eid]
	}
	return tree, nil
}

// ThreadAddStream add a stream to a thread.
func (t *Textile) ThreadAddStream(threadId string, streamId string) error {
	stream := t.GetStreamMeta(streamId)
	if stream == nil {
		return ErrStreamNotFound
	}
	thread := t.Thread(threadId)
	if thread == nil {
		return ErrThreadNotFound
	}
	_, err := thread.AddStreamMeta(stream)
	return err
}

// CloseStream close a stream already exist.
func (t *Textile) CloseStream(threadId string, streamId string) error {
	defer fmt.Printf("textile.CloseStream end success\n")
	fmt.Printf("textile.CloseStream\n")
	// TODO: send close message through thread
    stream := t.GetStreamMeta(streamId)
	if stream == nil {
        return &ErrStreamNotExist{Id: streamId}
	}

	t.stream.CloseStream(streamId)
    return nil
}

/* [deprecated] use stream_meta instead
func (t *Textile) GetStream(id string) *pb.Stream {
	return t.datastore.Streams().Get(id)
}
 */

// GetStreamMeta return a streamMeta from datastore.
func (t *Textile) GetStreamMeta(id string) *pb.StreamMeta {
	return t.datastore.StreamMetas().Get(id)
}

// ListStreamMeta return a list of streamMetas from datastore.
func (t *Textile) ListStreamMeta() *pb.StreamMetaList{
	return t.datastore.StreamMetas().List()
}

// StreamAddFile add the new file to the corresponding channel.
func (t *Textile) StreamAddFile(id string, sf *pb.StreamFile) error {
    err := t.stream.StreamAddFile(id, sf)
    if err != nil {
    	return err
	}
    //t.stream.sm.StreamAddFile(id, sf)
	return nil
}

func (t *Textile) ShadowSpeedSlow(intv int64){
	t.stream.SetInterval(intv)
}

// handleProviderSearchResult handle the result of SearchStream, try to connect the stream provider
// and request the stream.
func (t *Textile) handleProviderSearchResult(resultCh <-chan *pb.QueryResult, errCh <-chan error, cancel *broadcast.Broadcaster, config *pb.StreamRequest, sid string) (error) {
	log.Debugf("in handleProviderSearchResult")

    timer := time.NewTimer(time.Millisecond * 500) //Wait for 1s
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
        //log.Debug("Search time out")
		done()
	}()
	go func() {
		//defer cancel.Close()	// This will stop the existing pubsub queries.
								// However, only the searcher will stop listen it.
								// The responder may still response this pubsub query.
		for {
			select {
			case <-doneCh:
				log.Debugf("[%s] Stream %s", stream.TAG_STREAM_SEARCHTIMEOUT, config.Id)
				//log.Debugf("result channel done")
                t.SubscribeNotify(config.Id, false)

                go func() {
                	err := t.ReSubscribeStream(sid)
                	if err != nil {
                		log.Errorf("Stream %s %v", config.Id, err)
					}
                }()
                close(doneCh)
				return
			case err := <-errCh:
                log.Error(err)
                t.SubscribeNotify(config.Id, false)
				go func() {
					err := t.ReSubscribeStream(sid)
					if err != nil {
						log.Errorf("Stream %s %v", config.Id, err)
					}
				}()
				return

			case res, ok := <-resultCh:
                //log.Debug("get result!")
				if !ok {
                    log.Debug("Error occur")
					return
				}
				item := &pb.StreamQueryResultItem {}
				proto.Unmarshal(res.Value.Value, item)

                //if already have provider
                //just break
                if t.stream.GetProvider(config.Id) != peer.ID("") {
                    log.Debugf("Stream %s provider alread exists", config.Id)
                    done()
                    timer.Stop()
                    break
                }

                // if the provider is connected, request the stream directly
                //log.Debug("PID: "+item.Pid)
                connected, err := ipfs.SwarmConnected(t.node, item.Pid) 
	            if err != nil{
                    log.Error(err)
	    	        break
                }
                
                if connected {
                    //log.Debug("Try request stream")
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
                        //log.Errorf("request %s failed", item.Pid)
	    	            break
                    } else {
                        t.SubscribeNotify(config.Id, true)
						t.StreamRequestAccepted(item.Pid, config)
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

// SubscribeNotify print the log of subscribing a stream success or not,
// do not support substream currently.
func (t *Textile) SubscribeNotify(id string, res bool) {
    if res {
        log.Debugf("Subscribe stream "+id+" success")
    } else {
        log.Debugf("Subscribe stream "+id+" fail")
    }
}


func (t* Textile) ReSubscribeStream(id string) error {
	log.Debugf("[%s] Stream %s", stream.TAG_RETRY_SUBSCRIBE, id)
    retry, ok := num_retry_map[id]
    if !ok {
        retry = 0
    }
    if retry > MAX_RETRY {
    	//log.Errorf("Stream %s %s", )
        return ErrSubscribeFail 
    } else {
	    num_retry_map[id] = retry + 1
        return t.SubscribeStream(id)
    }
}

func (t* Textile) IsStreamFinished(id string) bool{
    meta := t.GetStreamMeta(id)
    bc := t.datastore.StreamBlocks().BlockCount(id)
    if meta!= nil && bc == meta.Nblocks && bc != 0 {
        return true
    }
    return false
}


// SubscribeStream calls SearchStream and handleProviderSearchResult to
// subscribe a stream, and shadow node will also subscribe the same stream.
func (t* Textile) SubscribeStream(id string) error {
    if t.stream.GetProvider(id) != peer.ID("") {
        return fmt.Errorf("Resubscribe stream "+id)
    }
	log.Debugf("[%s] Stream %s", stream.TAG_STREAMSUBSCRIBE, id)
    last := t.datastore.StreamBlocks().LastIndex(id)

    config := &pb.StreamRequest {
        Id: id,
        StreamMap: 1,
        StartIndex: last,
    }

    // shadow node should subscribe the same stream
    meta := t.GetStreamMeta(id)
	if meta != nil && !t.config.IsShadow {
        err := t.shadow.PushStreamMeta(meta, false)
        if err != nil {
        	log.Error(err)
		}
	}

	// if we already have all the blocks for this stream
	// TODO: Check what meta.Nblocks would be if this is a newly created stream
    if meta!= nil && last == meta.Nblocks && last != 0 {
        return nil
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
    err = t.handleProviderSearchResult(resCh, errCh, cancel, config, id)
    if err != nil {
    	log.Error(err)
    	return err
	}
	return nil
}

// UnsubscribeStream cancel subscribe to a stream.
func (t* Textile) UnsubscribeStream(id string) error{
    err := t.stream.UnsubscribeStream(id)
    return err
}

// RequestStream request a stream from a provider by sending stream request.
func (t* Textile) RequestStream(pid string, config *pb.StreamRequest) (*pb.Envelope, error){
	return t.stream.SendStreamRequest(pid, config)
}

func (t* Textile) StreamRequestAccepted(pid string, config *pb.StreamRequest) {
	t.stream.RequestAccepted(pid, config)
}

// SearchStream search a stream by pubsub in network.
func (t *Textile) SearchStream(query *pb.StreamQuery, options *pb.QueryOptions) (<-chan *pb.QueryResult, <-chan error, *broadcast.Broadcaster, error) {
	//log.Debug("in SearchStream")
	log.Debugf("[%s] Stream %s", stream.TAG_STREAMSEARCH, query.Id)
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

func (t *Textile) SetMaxWorkers(n int) {
	t.stream.SetMaxWorkers(n)
}

func (t *Textile) GetMaxWorkers() int {
	return t.stream.GetMaxWorkers()
}

func (t *Textile) StreamGetParent(sid string) string {
    return t.stream.GetParent(sid)
}

func (t *Textile) StreamGetStatusString(sid string) string {
	stat, ok := t.stream.GetStatus(sid)
	if ok {
		return stat.String()
	} else {
		return "UNKNOWN"
	}
}

func (t *Textile) GetDuration(streamId string) int64 {
	return t.stream.GetDuration(streamId)
}
