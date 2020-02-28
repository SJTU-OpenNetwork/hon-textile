package mobile

import (
	"bytes"
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
	return m.node.StartStream(thread, model)
}

func (m *Mobile) SubscribeStream(config []byte) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	model := new(pb.StreamRequest)
	if err := proto.Unmarshal(config, model); err != nil {
		return err
	}
	return m.node.SubscribeStream(model)
}

func (m *Mobile) UnsubscribeStream(streamid string) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	return m.node.UnsubscribeStream(streamid)
}

func (m *Mobile) StreamAddFile(streamid string, data []byte) error {
	file := bytes.NewReader(data)
	return m.node.StreamAddFile(streamid, file)
}

