package core

import (
"github.com/SJTU-OpenNetwork/hon-textile/pb"
"github.com/golang/protobuf/proto"
)
//收到block之后封装成feed类型，发给上层应用
func (t *Textile) feedStream(block *pb.Block, opts feedItemOpts) (*pb.FeedStreamMeta, error) {
	if block.Type != pb.Block_STREAMMETA {
		return nil, ErrBlockWrongType
	}

	msg := new(pb.Stream)
	err := proto.UnmarshalText(block.Body, msg)
	if err != nil {
		return nil, err
	}

	item := &pb.FeedStreamMeta{
		Block:   block.Id,
		Date:    block.Date,
		User:    t.PeerUser(block.Author),
		Stream:   msg,
	}

	return item, nil
}