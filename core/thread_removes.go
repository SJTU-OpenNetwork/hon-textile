package core

import (
	mh "github.com/multiformats/go-multihash"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
)

func (t *Thread) RemovePeer(peerId string) (mh.Multihash, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

    log.Debugf("TRY REMOVEPEER to %s for %s", peerId, t.Id)

    peer := t.datastore.Peers().Get(peerId)
    if peer.Address == t.account.Address(){
        log.Warning(ErrRemoveSelf)
        return nil, ErrRemoveSelf
    }

    // do remove peer from local db
	err := t.datastore.ThreadPeers().Delete(peerId, t.Id)
	if err != nil {
        log.Warning(ErrRemoveSelf)
	}
	err = t.datastore.Notifications().DeleteByActor(peerId)
	if err != nil {
        log.Warning(err)
	}

	msg := &pb.ThreadRemovePeer{
		Target: peerId,
	}
	res, err := t.commitBlock(msg, pb.Block_REMOVEPEER, true, nil)
	if err != nil {
        log.Warning(err)
		return nil, err
	}

    err = t.indexBlock(&pb.Block{
		Id:     res.hash.B58String(),
		Thread: t.Id,
		Author: res.header.Author,
		Type:   pb.Block_REMOVEPEER,
		Date:   res.header.Date,
		Status: pb.Block_QUEUED,
        Body:   peerId,
	}, false)
	if err != nil {
        log.Error(err)
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

    log.Debugf("handling remove peer, target: %s", msg.Target)
    peer := t.datastore.Peers().Get(msg.Target)
    if peer.Address == t.account.Address(){
        log.Debugf("handling remove peer, target is me!", msg.Target)
	    // do nothing, let uper level api handle this
//	    query := fmt.Sprintf("threadId='%s'", t.Id)
//	    for _, block := range t.datastore.Blocks().List("", -1, query).Items {
//            err := t.ignoreBlockTarget(block)
//	        if err != nil {
//	            return res, err
//	        }
//	    }
//        err := t.datastore.Blocks().DeleteByThread(t.Id)
//	    if err != nil {
//	        return res, err
//	    }
//	    err = t.datastore.ThreadPeers().DeleteByThread(t.Id)
//	    if err != nil {
//	        return res, err
//	    }
//	    err = t.datastore.Notifications().DeleteBySubject(t.Id)
//	    if err != nil {
//	        return res, err
//	    }
    } else {
        // do remove peer from local db
	    err := t.datastore.ThreadPeers().Delete(msg.Target, t.Id)
	    if err != nil {
		    log.Debug(err)
	    }
	    err = t.datastore.Notifications().DeleteByActor(msg.Target)
	    if err != nil {
		    log.Debug(err)
	    }
    }
    res.body = msg.Target
	return res, nil
}
