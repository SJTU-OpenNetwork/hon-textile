package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
)

func (t *Textile) addAdmin(block *pb.Block, opts feedItemOpts) (*pb.AddAdmin, error) {
	if block.Type != pb.Block_ADDADMIN {
		return nil, ErrBlockWrongType
	}

	item := &pb.AddAdmin{
		Block: block.Id,
		Date:  block.Date,
		User:  t.PeerUser(block.Author),
	    Target:block.Body,
    }

	return item, nil
}
