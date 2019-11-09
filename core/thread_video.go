package core

import (
	mh "github.com/multiformats/go-multihash"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

func (t *Thread) AddVideo(video *pb.Video) (mh.Multihash, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

    err := t.datastore.Videos().Add(video)
	if err != nil {
		return nil, err
	}

	res, err := t.commitBlock(video, pb.Block_VIDEO, true, nil)
	if err != nil {
		return nil, err
	}

    body, err := proto.Marshal(video)
	if err != nil {
		return nil, err
	}

    err = t.indexBlock(&pb.Block{
		Id:     res.hash.B58String(),
		Thread: t.Id,
		Author: res.header.Author,
		Type:   pb.Block_VIDEO,
		Date:   res.header.Date,
        Body:   string(body),
		Status: pb.Block_QUEUED,
	}, false)
	if err != nil {
		return nil, err
	}

    log.Debugf("added video %s, caption: %s", video.Id, video.Caption)
	return res.hash, nil
}


// handleAddVideoBlock handles an incoming add admin block
func (t *Thread) handleAddVideoBlock(block *pb.ThreadBlock) (handleResult, error) {
	var res handleResult

	if !t.readable(t.config.Account.Address) {
		return res, ErrNotReadable
	}
	if !t.readable(block.Header.Address) {
		return res, ErrNotReadable
	}

	msg := new(pb.Video)
	if block.Payload != nil {
		err := ptypes.UnmarshalAny(block.Payload, msg)
		if err != nil {
			return res, err
		}
	}

	err := t.datastore.Videos().Add(msg)
	if err != nil {
		return res, err
	}

    body, err := proto.Marshal(msg)
    if err != nil {
        log.Warning(err)
        return res, err
    }
    res.body = string(body)
	return res, nil
}
