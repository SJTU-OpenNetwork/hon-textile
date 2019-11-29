package mobile

import (
    "fmt"

	"github.com/golang/protobuf/proto"
	"github.com/SJTU-OpenNetwork/hon-textile/core"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
)

func (m *Mobile) AddVideo(video []byte) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	model := new(pb.Video)
	if err := proto.Unmarshal(video, model); err != nil {
		return err
	}

	return m.node.AddVideo(model)
}


func (m *Mobile) ThreadAddVideo(thread string, video string) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

    err := m.node.ConnectThreadPeers(thread)
	if err != nil {
        log.Error(err)
	}

    err = m.node.ThreadAddVideo(thread, video)
    if err != nil {
        return nil
    }
    m.node.FlushBlocks()
    return nil
}

func (m *Mobile) PublishVideo(video []byte) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	model := new(pb.Video)
	if err := proto.Unmarshal(video, model); err != nil {
		return err
	}
    return m.node.OLD_PublishVideo(model)
}

func (m *Mobile) PublishVideoChunk(vchunk []byte) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	model := new(pb.VideoChunk)
	if err := proto.Unmarshal(vchunk, model); err != nil {
		return err
	}
    return m.node.OLD_PublishVideoChunk(model)
}

func (m *Mobile) AddVideoChunk(vchunk []byte) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	model := new(pb.VideoChunk)
	if err := proto.Unmarshal(vchunk, model); err != nil {
		return err
	}

	return m.node.AddVideoChunk(model)
}



func (m *Mobile) GetVideo(id string) ([]byte, error) {
	if !m.node.Started() {
		return nil, core.ErrStopped
	}

	video := m.node.GetVideo(id)
	if video == nil {
		return nil, nil
	}

	return proto.Marshal(video)
}

func (m *Mobile) GetVideoChunk(id string, chunk string) ([]byte, error) {
	if !m.node.Started() {
		return nil, core.ErrStopped
	}

	vchunk := m.node.GetVideoChunk(id, chunk)
	if vchunk == nil {
		return nil, nil
	}

	return proto.Marshal(vchunk)
}

func (m *Mobile) GetVideoChunkByIndex(id string, index int64)([]byte, error){
	if !m.node.Started(){
		return nil, core.ErrStopped
	}

	vchunk := m.node.GetVideoChunkByIndex(id, index)
	if vchunk == nil {
		return nil, nil
	}
	return proto.Marshal(vchunk)
}

func (m *Mobile) ChunksByVideoId(id string) ([]byte, error) {
	if !m.node.Started() {
		return nil, core.ErrStopped
	}

	vchunks := m.node.ChunksByVideoId(id)
	return proto.Marshal(vchunks)
}

func (m *Mobile) RemoveVideo(id string) error {
	if !m.node.Started() {
		return core.ErrStopped
	}

	err := m.node.RemoveVideo(id)
	if err != nil {
		return err
	}
	return nil
}

func (m *Mobile) SearchVideo(query []byte, options []byte) (*SearchHandle, error) {
	if !m.node.Online() {
		return nil, core.ErrOffline
	}

	mquery := new(pb.VideoQuery)
	if err := proto.Unmarshal(query, mquery); err != nil {
		return nil, err
	}
	moptions := new(pb.QueryOptions)
	if err := proto.Unmarshal(options, moptions); err != nil {
		return nil, err
	}

	resCh, errCh, cancel, err := m.node.SearchVideo(mquery, moptions)
    if err != nil {
        log.Warning(err)
		return nil, err
	}
	return m.handleSearchStream(resCh, errCh, cancel)
}


func (m *Mobile) SearchVideoChunks(query []byte, options []byte) (*SearchHandle, error) {
	if !m.node.Online() {
		return nil, core.ErrOffline
	}

	mquery := new(pb.VideoChunkQuery)
	if err := proto.Unmarshal(query, mquery); err != nil {
		return nil, err
	}
	moptions := new(pb.QueryOptions)
	if err := proto.Unmarshal(options, moptions); err != nil {
		return nil, err
	}

	resCh, errCh, cancel, err := m.node.SearchVideoChunks(mquery, moptions)
    if err != nil {
        log.Warning(err)
		return nil, err
	}
	return m.handleSearchStream(resCh, errCh, cancel)
}

// StoreThread calls core StoreThread
func (m *Mobile) StoreThread() ([]byte, error) {
	if !m.node.Started() {
		return nil, core.ErrStopped
	}

	thrd := m.node.StoreThread()
	if thrd == nil {
		return nil, fmt.Errorf("store thread not found")
	}
	view, err := m.node.ThreadView(thrd.Id)
	if err != nil {
		return nil, err
	}

	return proto.Marshal(view)
}
