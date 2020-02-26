package core

import (
	"fmt"
//	"github.com/SJTU-OpenNetwork/hon-textile/stream"
	"github.com/ipfs/go-cid"

	//stream "github.com/SJTU-OpenNetwork/go-stream"
	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"io"
)
var ErrStreamNotFound = fmt.Errorf("stream not found")
var ErrStreamAlreadyInUse = fmt.Errorf("stream already in use")

func (t *Textile) TraverseNode(sid string, cid *cid.Cid) error {
    links, err := ipfs.LinksAtPath(t.node, cid.String())
    if len(links) == 0 {
        cur, ok := t.variables.streamBlockIndex[sid]
        if !ok {
            cur := 0
        }
        t.datastore.StreamBlocks.Add(&pb.StreamBlock{
            Id: cid.String(),
            Streamid: sid,
            Index: cur,
        })
        t.variables.streamBlockIndex[sid] = cur+1
    } else {
        for _,l := range links {
            t.TraverseNode(sid, &l.Cid)
        }
    }
    return nil
}

func (t *Textile) StartStream(threadId string, config *pb.StreamMeta) error {
	// if the stream id already in use?
	stream := t.GetStream(string(config.Id))
	if stream != nil {
		return ErrStreamAlreadyInUse
	}
    _, ok := t.variables.StreamFileChannels[config.Id]
    if ok {
		return ErrStreamAlreadyInUse
    }

    // TODO
    //init a Stream
	//stream :=stream.createstream()
	//err := t.datastore.Streams().Add()
	stream = t.GetStream(string(config.Id))
	if stream == nil {
		return ErrStreamNotFound
	}

    //Start a channel for adding files
    t.variables.StreamFileChannels[config.Id] = make(chan io.Reader)
    go func(){
	    for {
		    select {
            case  newfile := <-t.variables.StreamFileChannels[config.Id]:
                //filedata, err := ioutil.ReadAll(newfile)
                //if err != nil {
                //    log.Error(err)
                //    return
                //}
                fileid, err := ipfs.AddData(t.node, newfile, true, false)
                if err != nil {
                    log.Error(err)
                    return
                }
                err = t.TraverseNode(config.Id, fileid)
                if err != nil {
                    log.Error(err)
                    return
                }
                // TODO:
	            //solve fileid to ipld node
                //store blocks in stream_block 
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
	_, err := thread.AddStream(stream)
	return err
}

func (t *Textile) GetStream(id string) *pb.Stream {
	return t.datastore.Streams().Get(id)
}


// add the new file to the corresponding channel
func (t *Textile) StreamAddFile(id string, file io.Reader) error {
    t.variables.StreamFileChannels[id] <- file 
	return nil
}

func (t* Textile) SubscribeStream(config *pb.StreamRequest) error {
	// call search stream

	//t.SearchStream()
	// swarm connect publisher

	// call request stream
	//err := t.RequestStream(config)
	//if err!=nil {
	//	return err
	//}

	return nil
}

func (t* Textile) UnsubscribeStream(id string) error{
	return nil
}




func (t* Textile) RequestStream(pid peer.ID, config *pb.StreamRequest) error{
	return nil
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
