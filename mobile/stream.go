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

func (m *Mobile) StartStream_Text(thread string, stream []byte) error {
	model := new(pb.StreamMeta)
	if err := proto.Unmarshal(stream, model); err != nil {
		return err
	}
	err := m.node.StartStream_Text(thread, model)
	if err != nil {
		return err
	}

	m.node.FlushCafes()
	return nil
}

func (m *Mobile) FileAsStream_Text(thread string, sf []byte, file_type int) ([]byte, error) {
	model := new(pb.StreamFile)
	if err := proto.Unmarshal(sf, model); err != nil {
		return nil, err
	}
	meta, err := m.node.FileAsStream_Text(thread, model, pb.StreamMeta_Type(file_type))
	if err != nil {
		return nil, err
	}

	m.node.FlushCafes()
	metaByte, err := proto.Marshal(meta)
	if err != nil {
		return nil, err
	}
	return metaByte, nil
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

func (m *Mobile) SetMaxWorkers(n int) {
	m.node.SetMaxWorkers(n)
}

func (m *Mobile) GetMaxWorkers() int {
	return m.node.GetMaxWorkers()
}
