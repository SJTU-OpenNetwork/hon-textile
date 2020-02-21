package core

import (
	"fmt"

	stream "github.com/SJTU-OpenNetwork/go-stream"
	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	path "github.com/SJTU-OpenNetwork/interface-go-ipfs-core/path"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	peer "github.com/libp2p/go-libp2p-core/peer"
)
var ErrStreamNotFound = fmt.Errorf("stream not found")
func (t *Textile) StartStream(threadId string,config stream.StreamConfig) error {
	//init a Stream
	//stream :=stream.createstream()
	//err := t.datastore.Streams().Add()

	//publish the Stream to others
	thread := t.Thread(threadId)
	if thread == nil {
		return ErrStreamNotFound
	}
	stream := t.GetStream(config.ID)
	if stream == nil {
		return ErrStreamNotFound
	}
	_, err := thread.AddStream(stream)
	return err
	//start a stream
	//stream.startwork()
	//return nil
}



func (t *Textile) GetStream(id string) *pb.Stream {
	return t.datastore.Streams().Get(id)
}

func (t *Textile) StreamAddFile(id string, path path.Path) error {
	//solve path to ipld node
	stream := t.datastore.Streams().Get(id)
	cid := path.Cid(path.Resolved())
	block:=
	//call stream.Addfile
	AddJob(stream,block)

	return nil
}

func (t* Textile) SubscribeStream(id string) error {
	// call search stream

	t.SearchStream()
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
