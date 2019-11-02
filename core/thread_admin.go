package core

import (
	mh "github.com/multiformats/go-multihash"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
)

func (t *Thread) AddAdmin(peerId string) (mh.Multihash, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	err := t.datastore.ThreadPeers().AddAdmin(t.Id, peerId)
	if err != nil {
		return nil, err
	}

	msg := &pb.ThreadAddAdmin{
		Target: peerId,
	}
	res, err := t.commitBlock(msg, pb.Block_ADDADMIN, true, nil)
	if err != nil {
		return nil, err
	}

    err = t.indexBlock(&pb.Block{
		Id:     res.hash.B58String(),
		Thread: t.Id,
		Author: res.header.Author,
		Type:   pb.Block_ADDADMIN,
		Date:   res.header.Date,
		Target: peerId,
		Status: pb.Block_QUEUED,
	}, false)
	if err != nil {
		return nil, err
	}

    log.Debugf("added ADDADMIN to %s for %s", peerId, t.Id)

	return res.hash, nil
}

// handleAddAdminBlock handles an incoming add admin block
func (t *Thread) handleAddAdminBlock(block *pb.ThreadBlock) (handleResult, error) {
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

	err := t.datastore.ThreadPeers().AddAdmin(t.Id, msg.Target)
	if err != nil {
		return res, err
	}

	return res, nil
}
