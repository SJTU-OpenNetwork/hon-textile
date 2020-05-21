package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	mh "github.com/multiformats/go-multihash"
)

func (t *Thread) AddSimpleFile(file *pb.SimpleFile) (mh.Multihash, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	res, err := t.commitBlock(file, pb.Block_SIMPLE_FILE, true, nil)
	if err != nil {
		return nil, err
	}

	body := proto.MarshalTextString(video)

	err = t.indexBlock(&pb.Block{
		Id:     res.hash.B58String(),
		Thread: t.Id,
		Author: res.header.Author,
		Type:   pb.Block_VIDEO,
		Date:   res.header.Date,
		Body:   body,
		Status: pb.Block_QUEUED,
	}, false)
	if err != nil {
		return nil, err
	}

	log.Debugf("added video %s, caption: %s", video.Id, video.Caption)
	return res.hash, nil
}
