package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/proto"
)

func (t *Textile) feedVideo(block *pb.Block, opts feedItemOpts) (*pb.FeedVideo, error) {
	if block.Type != pb.Block_VIDEO {
		return nil, ErrBlockWrongType
	}

    msg := new(pb.Video)
	err := proto.Unmarshal([]byte(block.Body), msg)
	if err != nil {
		return nil, err
	}

	item := &pb.FeedVideo{
		Block:   block.Id,
		Date:    block.Date,
		User:    t.PeerUser(block.Author),
        Video:   msg,
    }

	return item, nil
}
