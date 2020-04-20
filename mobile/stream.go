package mobile

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/proto"

	"github.com/SJTU-OpenNetwork/hon-textile/core"
)


func (m *Mobile) StartStream(thread string, stream []byte) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	model := new(pb.StreamMeta)
	if err := proto.Unmarshal(stream, model); err != nil {
		return err
	}
    err := m.node.StartStream(thread, model)
	if err != nil {
		return err
	}

	m.node.FlushCafes()
	return nil
}

func (m *Mobile) SubscribeStream(config string) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	//model := new(pb.StreamRequest)
	//if err := proto.Unmarshal(config, model); err != nil {
	//	return err
	//}
	return m.node.SubscribeStream(config)
}

func (m *Mobile) UnsubscribeStream(streamid string) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	return m.node.UnsubscribeStream(streamid)
}

func (m *Mobile) StreamAddFile(streamid string, f []byte) error {
	if !m.node.Started() {
		return core.ErrStopped
	}
	file := new(pb.StreamFile)
	if err := proto.Unmarshal(f, file); err != nil {
		return err
	}
	return m.node.StreamAddFile(streamid, file)
}

func (m *Mobile) CloseStream(threadId string, streamId string) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	return m.node.CloseStream(threadId,streamId)
}

func (m *Mobile) ThreadAddStream(threadId string, streamId string) error{
	if !m.node.Started() {
		return core.ErrStopped
	}

	return m.node.ThreadAddStream(threadId,streamId)
}

