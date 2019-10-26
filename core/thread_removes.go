package core

import (
	mh "github.com/multiformats/go-multihash"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
    "fmt"
)

func (t *Thread) RemovePeer(peerId string) (mh.Multihash, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

    peer := t.datastore.Peers().Get(peerId)
    if peer.Address == t.account.Address(){
        return nil, ErrRemoveSelf
    }

    // do remove peer from local db
	err := t.datastore.ThreadPeers().Delete(peerId, t.Id)
	if err != nil {
		return nil, err
	}
	err = t.datastore.Notifications().DeleteByActor(peerId)
	if err != nil {
		return nil, err
	}

	res, err := t.commitBlock(nil, pb.Block_REMOVEPEER, true, nil)
	if err != nil {
		return nil, err
	}

    log.Debugf("added REMOVEPEER to %s for %s", peerId, t.Id)

	return res.hash, nil
}

// handleAddAdminBlock handles an incoming add admin block
func (t *Thread) handleRemovePeerBlock(block *pb.ThreadBlock) (handleResult, error) {
	var res handleResult

	if !t.readable(t.config.Account.Address) {
		return res, ErrNotReadable
	}
	if !t.readable(block.Header.Address) {
		return res, ErrNotReadable
	}

	msg := new(pb.ThreadRemovePeer)
	if block.Payload != nil {
		err := ptypes.UnmarshalAny(block.Payload, msg)
		if err != nil {
			return res, err
		}
	}

    peer := t.datastore.Peers().Get(block.Header.Author)
    if peer.Address == t.account.Address(){
	    // cleanup
	    query := fmt.Sprintf("threadId='%s'", t.Id)
	    for _, block := range t.datastore.Blocks().List("", -1, query).Items {
            err := t.ignoreBlockTarget(block)
	        if err != nil {
	            return res, err
	        }
	    }
        err := t.datastore.Blocks().DeleteByThread(t.Id)
	    if err != nil {
	        return res, err
	    }
	    err = t.datastore.ThreadPeers().DeleteByThread(t.Id)
	    if err != nil {
	        return res, err
	    }
	    err = t.datastore.Notifications().DeleteBySubject(t.Id)
	    if err != nil {
	        return res, err
	    }
    } else {
        // do remove peer from local db
	    err := t.datastore.ThreadPeers().Delete(block.Header.Author, t.Id)
	    if err != nil {
		    return res, err
	    }
	    err = t.datastore.Notifications().DeleteByActor(block.Header.Author)
	    if err != nil {
		    return res, err
	    }
    }
	return res, nil
}
