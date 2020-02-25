package mobile

import (
    "fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/proto"

	"github.com/SJTU-OpenNetwork/hon-textile/core"
)

func (m *Mobile) InsertTestStream(streamMeta []byte) {
	mStreamMeta := new(pb.StreamMeta)
	proto.Unmarshal(streamMeta, mStreamMeta)
	m.node.Datastore().StreamMetas().Add(mStreamMeta)
	mStream := &pb.Stream{
		Id: mStreamMeta.Id,
	}
	m.node.Datastore().Streams().Add(mStream)
}

func (m *Mobile) AndroidTestSearchStream(query []byte, options []byte) (*SearchHandle, error) {
	mquery := new(pb.StreamQuery)
	if err := proto.Unmarshal(query, mquery); err != nil {
		return nil, err
	}
	moptions := new(pb.QueryOptions)
	if err := proto.Unmarshal(options, moptions); err != nil {
		return nil, err
	}

	resCh, errCh, cancel, err := m.node.SearchStream(mquery,moptions)
	if err != nil {
		log.Warning(err)
		return nil, err
	}
	return m.handleSearchStream(resCh, errCh, cancel)
}



//func (m *Mobile) StartStream() error {
//	if !m.node.Started() {
//		return core.ErrStopped
//	}
//
//	return m.node.StartStream(model)
//}
//
//func (m *Mobile) SubscribeStream() error {
//	if !m.node.Started() {
//		return core.ErrStopped
//	}
//
//	return m.node.SubscribeStream(model)
//}
//
//func (m *Mobile) UnsubscribeStream() error {
//	if !m.node.Started() {
//		return core.ErrStopped
//	}
//
//	return m.node.UnsubscribeStream(model)
//}
//
