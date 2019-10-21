package core

import (
	"github.com/textileio/go-textile/pb"
)

func (t *Textile) addAdmin(block *pb.Block, opts feedItemOpts) (*pb.AddAdmin, error) {
	if block.Type != pb.Block_ADDADMIN {
		return nil, ErrBlockWrongType
	}

	item := &pb.AddAdmin{
		Block: block.Id,
		Date:  block.Date,
		User:  t.PeerUser(block.Author),
	}

	return item, nil
}
