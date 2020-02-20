package core
import (
	"github.com/gogo/protobuf/proto"
	mh "github.com/multiformats/go-multihash"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
)



func (t *Thread) AddStream(stream *pb.Stream) (mh.Multihash, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	res, err := t.commitBlock(stream, pb.Block_STREAM, true, nil)
	if err != nil {
		return nil, err
	}

	body := proto.MarshalTextString(stream)

	err = t.indexBlock(&pb.Block{
		Id:     res.hash.B58String(),
		Thread: t.Id,
		Author: res.header.Author,
		Type:   pb.Block_STREAM,
		Date:   res.header.Date,
		Body:   body,
		Status: pb.Block_QUEUED,
	}, false)
	if err != nil {
		return nil, err
	}

	log.Debugf("added stream %s",stream.Id)
	return res.hash, nil
}