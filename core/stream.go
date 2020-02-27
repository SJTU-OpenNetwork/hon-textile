package core

import (
	"fmt"
	"time"

	//	"github.com/SJTU-OpenNetwork/hon-textile/stream"
	"github.com/ipfs/go-cid"

	//stream "github.com/SJTU-OpenNetwork/go-stream"
	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"io"
)
var ErrStreamNotFound = fmt.Errorf("stream not found")
var ErrStreamAlreadyInUse = fmt.Errorf("stream already in use")

func (t *Textile) TraverseNode(sid string, cid *cid.Cid, bool isRoot) error {
    links, err := ipfs.LinksAtPath(t.node, cid.String())
    if err != nil{
        return err
    }
    if len(links) == 0 {
        cur, ok := t.variables.streamBlockIndex[sid]
        if !ok {
            cur = 0
        }

        stat, err := ipfs.StatObjectAtPath(t.node, cid.String())
        t.datastore.StreamBlocks().Add(&pb.StreamBlock{
            Id: cid.String(),
            Streamid: sid,
            Index: cur,
            Size: stat.Size(),
            IsRoot: isRoot,
        })
        t.variables.streamBlockIndex[sid] = cur+1
    } else {
        for _,l := range links {
            t.TraverseNode(sid, &l.Cid, false)
        }
    }
    return nil
}

func (t *Textile) StartStream(threadId string, config *pb.StreamMeta) error {
	// if the stream id already in use?
	stream := t.GetStreamMeta(string(config.Id))
	if stream != nil {
		return ErrStreamAlreadyInUse
	}
    _, ok := t.variables.StreamFileChannels[config.Id]
    if ok {
		return ErrStreamAlreadyInUse
    }

    // TODO
    //init a Stream
	err := t.datastore.StreamMetas().Add(config)
	stream = t.GetStreamMeta(string(config.Id))
	if stream == nil {
		return ErrStreamNotFound
	}
    t.variables.streamBlockIndex[config.Id] = 0

    //Start a channel for adding files
    t.variables.StreamFileChannels[config.Id] = make(chan io.Reader)
    go func(){
	    for {
		    select {
            case  newfile := <-t.variables.StreamFileChannels[config.Id]:
                fileid, err := ipfs.AddData(t.node, newfile, true, false)
                if err != nil {
                    log.Error(err)
                    return
                }
                err = t.TraverseNode(config.Id, fileid, true)
                if err != nil {
                    log.Error(err)
                    return
                }
		    case <-t.done:
			    return
		    }
	    }
    }()

	//publish the Stream to others
	thread := t.Thread(threadId)
	if thread == nil {
		return ErrStreamNotFound
	}
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
func (t *Textile) StreamAddFile(id string, file io.Reader) error {
    t.variables.StreamFileChannels[id] <- file 
	return nil
}

func (t* Textile) SubscribeStream(config *pb.StreamRequest) error {
	// call search stream
    query := & pb.StreamQuery { 
        Id: config.Id,
    }
    opt := &pb.QueryOptions {
        Wait: 10,
        Limit: 10,
    }
    timer := time.NewTimer(time.Second) //Wait for 1s or find 3 sources
    resCh, errCh, cancel, err := t.SearchStream(query, opt)
    if err != nil {
        return err
    }
    sources := []string
    doneCh := make(chan struct{})
	done := func() {
		close(doneCh)
    }
    go func() {
		<-timer.C
		done()
	}()

    for {
		select {
		case <-doneCh:
			break
		case value, ok := <-resCh:
			if !ok {
                log.Warning("error in SubscribeStream (search)")
                done()
                break
            }
            sources = append(sources, value)
            if len(sources) > 3 {
                done()
                break
            }
        }
    }

    if len(sources) == 0 {
        return fmt.Error("Cannot locate sources")
    }

    for _, source := sources {
	    // swarm connect publisher
        t.TryConnect(source)

        err = t.RequestStream(source, config)
	    if err!=nil {
            log.Errorf("request %s failed", source)
            log.Errorf(err)
	    	continue
	    }
    }
	return fmt.Error("Subscribe failed!")
}

func (t* Textile) UnsubscribeStream(id string) error{
	return nil
}

func (t* Textile) RequestStream(pid string, config *pb.StreamRequest) (*pb.Envelope, error){
	return t.stream.SendStreamRequest(pid, config)
}


func (t *Textile) SearchStream(query *pb.StreamQuery, options *pb.QueryOptions) (<-chan *pb.QueryResult, <-chan error, *broadcast.Broadcaster, error) {
	log.Debug("in SearchStream")
	payload, err := proto.Marshal(query)
	if err != nil {
		return nil, nil, nil, err
	}

	options.Filter = pb.QueryOptions_HIDE_OLDER

	resCh, errCh, cancel := t.search(&pb.Query{
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

//TODO:
// - Add Stream into database
// - Implement search for pb.Query_VIDEO in core/cafe_service.go/searchLocal.
// - Add call back function to bitswap when initialize bitswap.
