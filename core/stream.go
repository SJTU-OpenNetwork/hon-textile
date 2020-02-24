package core

import (
	"fmt"
	stream "github.com/SJTU-OpenNetwork/go-stream"
	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"io"
)
var ErrStreamNotFound = fmt.Errorf("stream not found")
var ErrStreamAlreadyInUse = fmt.Errorf("stream already in use")

func (t *Textile) StartStream(threadId string,config stream.StreamConfig) error {
	// if the stream id already in use?
	stream := t.GetStream(string(config.StreamID))
	if stream != nil {
		return ErrStreamAlreadyInUse
	}
    _, ok := t.variables.StreamFileChannels[config.StreamID]
    if ok {
		return ErrStreamAlreadyInUse
    }

    //init a Stream
	//stream :=stream.createstream()
	//err := t.datastore.Streams().Add()
	stream = t.GetStream(string(config.StreamID))
	if stream == nil {
		return ErrStreamNotFound
	}

    //Start a channel for adding files
    t.variables.StreamFileChannels[config.StreamID] = make(chan io.Reader)
    go func(){
	    for {
		    select {
            //case  newfile := <-t.variables.StreamFileChannels[config.StreamID]: // if not comment it out, the compiler will show "declared but not used"
                //fileid, err := ipfs.AddData(t.node, newfile, true, false)
                // TODO:
	            //solve fileid to ipld node
                //store blocks in stream_block 
		    case <-t.done:
			    return
		    }
	    }
    }()

    // Call go-stream StartStream
    //ipfs.StartStream(t.node, stream)

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

func (t* Textile) SubscribeStream(id string) error {
	// call search stream

	//t.SearchStream()
	// swarm connect publisher

	// call request stream
	err := t.RequestStream(id)
	if err!=nil {
		return err
	}
	// call stream.StartWorker

	return nil
}

func (t* Textile) UnsubscribeStream(id string) error{
	return nil
}




func (t* Textile) RequestStream(id string) error{

	return nil
}

// Handle request of streamid from peerid
func (t *Textile) HandleRequestStream(streamid string, peerid peer.ID) error {

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

//TODO:
// - Add Stream into database
// - Implement search for pb.Query_VIDEO in core/cafe_service.go/searchLocal.
// - Add call back function to bitswap when initialize bitswap.
