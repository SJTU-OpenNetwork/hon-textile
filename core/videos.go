package core

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/SJTU-OpenNetwork/hon-textile/broadcast"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"

    "fmt"
)

var ErrVideoNotFound = fmt.Errorf("video not found")

func (t *Textile) AddVideo(video *pb.Video) error {
    err := t.datastore.Videos().Add(video)
	if err != nil {
        log.Debug("should not get here!")
		return err
	}
	return nil
}


func (t *Textile) ThreadAddVideo(threadId string, videoId string) error {
    thread := t.Thread(threadId)
	if thread == nil {
		return ErrThreadNotFound
	}
 
    video := t.GetVideo(videoId)
    if video == nil {
        return ErrVideoNotFound
    }

    _, err := thread.AddVideo(video)
    return err
}

func (t *Textile) PublishVideo(video *pb.Video) error {
	sessions := t.datastore.CafeSessions().List().Items
	if len(sessions) == 0 {
		return nil
	}
	for _, session := range sessions {
		if err := t.cafe.PublishVideo(video, session.Id); err != nil {
			return err
		}
	}
	return nil
}

func (t *Textile) PublishVideoChunk(vchunk *pb.VideoChunk) error {
	sessions := t.datastore.CafeSessions().List().Items
	if len(sessions) == 0 {
		return nil
	}
	for _, session := range sessions {
		if err := t.cafe.PublishVideoChunk(vchunk, session.Id); err != nil {
			return err
		}
	}
	return nil
}

func (t *Textile) AddVideoChunk(vchunk *pb.VideoChunk) error {
	c := t.datastore.VideoChunks().Get(vchunk.Id, vchunk.Chunk)
    if c != nil {
        return nil
    }

    err := t.datastore.VideoChunks().Add(vchunk)
	if err != nil {
        log.Debug("should not get here!")
		return err
	}
	return nil
}

func (t *Textile) GetVideo(id string) *pb.Video {
	return t.datastore.Videos().Get(id)
}

func (t *Textile) GetVideoChunk(videoId string, chunk string) *pb.VideoChunk {
	return t.datastore.VideoChunks().Get(videoId, chunk)
}

func (t *Textile) RemoveVideo(id string) error {
    err := t.datastore.VideoChunks().Delete(id)
    if err != nil{
        log.Warning(err)
        return err
    }
    return t.datastore.Videos().Delete(id)
}

// SearchVideoChunks searches the network for videoChunks
func (t *Textile) SearchVideoChunks(query *pb.VideoChunkQuery, options *pb.QueryOptions) (<-chan *pb.QueryResult, <-chan error, *broadcast.Broadcaster, error) {
	payload, err := proto.Marshal(query)
	if err != nil {
		return nil, nil, nil, err
	}

	// settings required for contacts
	options.Filter = pb.QueryOptions_HIDE_OLDER

	resCh, errCh, cancel := t.search(&pb.Query{
		Type:    pb.Query_CONTACTS,
		Options: options,
		Payload: &any.Any{
			TypeUrl: "/VideoChunkQuery",
			Value:   payload,
		},
	})
	return resCh, errCh, cancel, nil
}

func (t *Textile) ChunksByVideoId(videoId string) *pb.VideoChunkList {
	vchunks := &pb.VideoChunkList{Items: make([]*pb.VideoChunk, 0)}
	for _, c := range t.datastore.VideoChunks().ListByVideo(videoId) {
		vchunks.Items = append(vchunks.Items, c)
	}
    return vchunks
}

