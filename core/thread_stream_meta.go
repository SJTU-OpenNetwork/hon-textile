package core

import (
    "fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/util"
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

func (t *Thread) AddStreamMeta_Text(stream *pb.StreamMeta) (mh.Multihash, error){
	t.lock.Lock()
	defer t.lock.Unlock()
	body := proto.MarshalTextString(stream)
	body = fmt.Sprintf("![%s]%s", util.CMD_STREAM_META, body)
	log.Debugf("Text command built: %s\n", body)

	msg := &pb.ThreadMessage{
		Body: body,
	}

	res, err := t.commitBlock(msg, pb.Block_TEXT, true, nil)
	if err != nil {
		return nil, err
	}

	err = t.indexBlock(&pb.Block{
		Id:     res.hash.B58String(),
		Thread: t.Id,
		Author: res.header.Author,
		Type:   pb.Block_TEXT,
		Date:   res.header.Date,
		Target: "",
		Body:   msg.Body,
		Status: pb.Block_QUEUED,
	}, false)
	if err != nil {
		return nil, err
	}

	log.Debugf("added stream through message to %s: %s", t.Id, res.hash.B58String())
	return res.hash, nil
}

func (t *Thread) handleAddStreamMetaBlock(block *pb.ThreadBlock) (handleResult,error){ 
	defer fmt.Printf("Finish handelAddStreamMetaBlock\n")
	fmt.Printf("In handelAddStreamMetaBlock\n")

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
