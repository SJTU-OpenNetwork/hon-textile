package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	mh "github.com/multiformats/go-multihash"
)

func (t *Thread) AddStreamMeta(stream *pb.StreamMeta) (mh.Multihash, error){
	t.lock.Lock()
	defer t.lock.Unlock()

	res, err := t.commitBlock(stream, pb.Block_STREAMMETA, true, nil)
	if err != nil {
		return nil, err
	}

	body := proto.MarshalTextString(stream)

	err = t.indexBlock(&pb.Block{
		Id: res.hash.B58String(),
		Thread: t.Id,
		Author: res.header.Author,
		Type: pb.Block_STREAMMETA,
		Date: res.header.Date,
		Body: body,
		Status: pb.Block_QUEUED,
	},false)
	if err != nil {
		return nil, err
	}

	log.Debugf("added streammeta, streamID: &s", stream.Id)
	return res.hash, nil
}

func (t *Thread) handleAddStreamMetaBlock(block *pb.ThreadBlock) (handleResult,error){
	var res handleResult

	if !t.readable(t.config.Account.Address) {
		return res, ErrNotReadable
	}
	if !t.readable(block.Header.Address) {
		return res, ErrNotReadable
	}

	msg := new(pb.StreamMeta)
	if block.Payload != nil {
		err := ptypes.UnmarshalAny(block.Payload, msg)
		if err != nil {
			return res, err
		}
	}

	err := t.datastore.StreamMetas().Add(msg)
	if err != nil {
		log.Warning(err)
	}

	body := proto.MarshalTextString(msg)
	res.body = body
	return res, nil
}
