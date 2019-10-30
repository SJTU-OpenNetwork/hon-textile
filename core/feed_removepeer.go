package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
)

func (t *Textile) removePeer(block *pb.Block, opts feedItemOpts) (*pb.RemovePeer, error) {
	if block.Type != pb.Block_REMOVEPEER {
		return nil, ErrBlockWrongType
	}

	item := &pb.RemovePeer{
		Block: block.Id,
		Date:  block.Date,
		User:  t.PeerUser(block.Author),
	    Target:block.Target,
    }

	return item, nil
}
