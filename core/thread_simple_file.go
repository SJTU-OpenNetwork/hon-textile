package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

func (t *Thread) AddSimpleFile(file *pb.SimpleFile) (*pb.Block, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	log.Debugf("Thread.AddSimpleFile")
	res, err := t.commitBlock(file, pb.Block_SIMPLE_FILE, true, nil)
	if err != nil {
		return nil, err
	}

	body := proto.MarshalTextString(file)
	block := &pb.Block{
		Id:     res.hash.B58String(),
		Thread: t.Id,
		Author: res.header.Author,
		Type:   pb.Block_SIMPLE_FILE,
		Date:   res.header.Date,
		Body:   body,
		Status: pb.Block_QUEUED,
	}
	err = t.indexBlock(block, false)
	if err != nil {
		return nil, err
	}

	log.Debugf("Done Thread.AddSimpleFile: %s", block.Id)
	return block, nil
}


func (t *Thread) handleSimpleFile(block *pb.ThreadBlock) (handleResult,error){

	var res handleResult

	if !t.readable(t.config.Account.Address) {
		return res, ErrNotReadable
	}
	if !t.readable(block.Header.Address) {
		return res, ErrNotReadable
	}

	msg := new(pb.SimpleFile)
	if block.Payload != nil {
		err := ptypes.UnmarshalAny(block.Payload, msg)
		if err != nil {
			return res, err
		}
	}

	// SEND NOTIFICATION??

	body := proto.MarshalTextString(msg)
	res.body = body
	return res, nil
}
