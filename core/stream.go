package core

import (
	stream "github.com/SJTU-OpenNetwork/go-stream"
	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	path "github.com/SJTU-OpenNetwork/interface-go-ipfs-core/path"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func (t *Textile) StartStream(threadId string,config stream.StreamConfig) error{
	//init a Stream
	//stream :=stream.createstream()
	//err := t.datastore.Streams().Add()

	//publish the Stream to others
	thread := t.Thread(threadId)
	if thread == nil {
		return ErrThreadNotFound
	}

	stream := t.datastore.Streams().Get(config.ID)
	video := t.GetVideo(videoId)
	if video == nil {
		return ErrVideoNotFound
	}
	_, err := thread.AddVideo(video)
	return err
	//start a stream
	stream.startwork()
	return nil
}

func (t *Textile) StreamAddFile(id uint64, path path.Path) error {
	//solve path to ipld node
	ipfsPath :=ResolveIPNS()
	//call stream.Addfile
	return nil
}

func (t* Textile) SubscribeStream(id uint64) error {
	// call search stream

	// swarm connect publisher

	// call request stream

	// call stream.StartWorker
	return nil
}

func (t* Textile) UnsubscribeStream(id uint64) error{
	return nil
}

func (t* Textile) RequestStream(id uint64) error{
	return nil
}

// Handle request of streamid from peerid
func (t *Textile) HandleRequestStream(streamid uint64, peerid peer.ID) error {

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
