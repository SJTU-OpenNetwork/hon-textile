package core

import (
	"fmt"

	mh "github.com/multiformats/go-multihash"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
)

func (t *Thread) AddAdmin(target string) (mh.Multihash, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if !t.readable(t.config.Account.Address) {
		return nil, ErrNotReadable
	}

	res, err := t.commitBlock(nil, pb.Block_ADMIN, true, nil)
	if err != nil {
		return nil, err
	}

    log.Debugf("added ADDADMIN to %s for %s", target, t.Id)

	return res.hash, nil
}

// handleLeaveBlock handles an incoming leave block
func (t *Thread) handleSetAdminBlock(block *pb.ThreadBlock) (handleResult, error) {
	var res handleResult

	if !t.readable(t.config.Account.Address) {
		return res, ErrNotReadable
	}
	if !t.readable(block.Header.Address) {
		return res, ErrNotReadable
	}

	msg := new(pb.ThreadAddAdmin)
	if block.Payload != nil {
		err := ptypes.UnmarshalAny(block.Payload, msg)
		if err != nil {
			return res, err
		}
	}

	err := t.datastore.ThreadPeers().SetAdmin(msg.Target, t.Id)
	if err != nil {
		return res, err
	}

	return res, nil
}
