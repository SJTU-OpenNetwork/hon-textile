package core

import (
	"fmt"
	"time"
	//	"github.com/SJTU-OpenNetwork/hon-textile/stream"
	"github.com/ipfs/go-cid"

	//stream "github.com/SJTU-OpenNetwork/go-stream"
	"github.com/golang/protobuf/ptypes"
	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
)
var ErrStreamNotFound = fmt.Errorf("stream not found")
var ErrStreamAlreadyInUse = fmt.Errorf("stream already in use")



func (t *Textile) StartStream(threadId string, config *pb.StreamMeta) error {
	defer fmt.Printf("textile.StartStream end success\n")
	fmt.Printf("textile.StartStream\n")
    
    err := t.datastore.StreamMetas().Add(config)
    if err != nil {
        return err
    }
    stream := t.GetStreamMeta(string(config.Id))
	if stream == nil {
		return ErrStreamNotFound
	}
    
    t.stream.StartStream(config)

	//publish the Stream to others
	fmt.Printf("Find thread for stream.\n")
	thread := t.Thread(threadId)
	if thread == nil {
		return ErrStreamNotFound
	}

	//fmt.Printf("Add streamMeta to thread.\n")
	_, err = thread.AddStreamMeta(stream)
	return err
}

func (t *Textile) GetStream(id string) *pb.Stream {
	return t.datastore.Streams().Get(id)
}

func (t *Textile) GetStreamMeta(id string) *pb.StreamMeta {
	return t.datastore.StreamMetas().Get(id)
}


// add the new file to the corresponding channel
func (t *Textile) StreamAddFile(id string, sf *pb.StreamFile) error {
    t.stream.StreamAddFile(id, sf)
    //t.stream.sm.StreamAddFile(id, sf)
	return nil
}

func (t *Textile) handleSearchProvider(resultCh <-chan *pb.QueryResult, errCh <-chan error, cancel *broadcast.Broadcaster, config *pb.StreamRequest) (error) {
	go func() {
		for {
			select {
			case err := <-errCh:
                log.Error(err)
				return

			case res, ok := <-resultCh:
				if !ok {
					return
				}
				log.Debugf("get search result , id: %s",res.Id)

                //if already have provider
                //just break
                if t.stream.GetProvider(config) != nil {
                    item :=new(pb.StreamQueryResultItem)
                    err := proto.Unmarshal(res.Value.Value, item)
	                if err!=nil {
                        log.Error(err)
	    	            break
	                }

                    t.stream.AddPotential(item.Pid, config, int(item.Hopcnt))
                    break
                }

                // if the provider is connected, request the stream directly
                connected, err := ipfs.SwarmConnected(t.node, res.Id) 
	            if err != nil{
                    log.Error(err)
	    	        break
                }
                if connected {

                    env, err := t.RequestStream(res.Id, config)
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
                        log.Errorf("request %s failed", res.Id)
	    	            break
                    }
                }
                // what if the remote peer is not connected?
                // request the stream from peers in the potentialProviderList?
			}
		}
	}()
	return nil
}

func (t* Textile) SubscribeStream(id string) error {
    config := &pb.StreamRequest {
        Id: id,
        StreamMap: 1,
        StartIndex: 0,
    }
	// call search stream
    query := & pb.StreamQuery { 
        Id: id,
    }
    opt := &pb.QueryOptions {
        Wait: 10,
        Limit: 10,
    }
    timer := time.NewTimer(time.Second) //Wait for 1s or find 3 sources
    //resCh, errCh, cancel, err := t.SearchStream(query, opt)
    // TODO: Unhandled channel
	resCh, _, _, err := t.SearchStream(query, opt)
    if err != nil {
        return err
    }
    
    sources := make([]string, 0)
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
		done()

	}()
	// break will only break select if there is no Label L
	L:
		for {
			select {
			case <-doneCh:
				break L
			case value, ok := <-resCh:
				if !ok {
					log.Warning("error in SubscribeStream (search)")
					done()
					break L
				}
				// TODO: Fix this bug.
				//		Value.Id should be peerid.
				//		But now it is streamId!!!!!
				res := &pb.StreamQueryResultItem {}
				proto.Unmarshal(value.Value.Value, res)
				//sources = append(sources, value.Id)
				sources = append(sources, res.Pid)
				if len(sources) > 3 {
					done()
					break L
				}
			}
		}

    if len(sources) == 0 {
        return fmt.Errorf("Cannot locate sources")
    }

    for _, source := range sources {
	    // swarm connect publisher
        t.TryConnect(source)
		fmt.Printf("core/stream.go SubscribeStream: Send stream request to %s\n", source)
        env, err := t.RequestStream(source, config)
	    if err != nil{
            log.Errorf("request %s failed", source)
            log.Error(err)
	    	continue
        }
        response := new(pb.StreamRequestHandle)
        err = ptypes.UnmarshalAny(env.Message.Payload, response)
	    if err!=nil {
            log.Errorf("request %s failed", source)
            log.Error(err)
	    	continue
	    }
        if response.Value != 1 {
            log.Errorf("request %s failed", source)
	    	continue
        }

        // request accepted by receiver.

        return nil
    }
	return fmt.Errorf("Subscribe failed!")
}

func (t* Textile) UnsubscribeStream(id string) error{
	return nil
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

	resCh, errCh, cancel := t.searchAll(&pb.Query{
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

func (t *Textile) StreamClose(streamId string) {
	t.stream.CloseStream(streamId)
}
