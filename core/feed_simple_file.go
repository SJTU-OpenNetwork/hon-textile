package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/proto"
)

func (t *Textile) feedSimpleFile(block *pb.Block, opts feedItemOpts) (*pb.FeedVideo, error) {
	if block.Type != pb.Block_SIMPLE_FILE {
		return nil, ErrBlockWrongType
	}

	msg := new(pb.SimpleFile)
	err := proto.UnmarshalText(block.Body, msg)
	if err != nil {
		return nil, err
	}

	item := &pb.FeedSimpleFile{
		Block:   block.Id,
		Date:    block.Date,
		User:    t.PeerUser(block.Author),
		SimpleFile:   msg,
	}

	return item, nil
}
