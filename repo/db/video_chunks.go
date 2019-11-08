package db

import (
	"database/sql"
	"sync"
    "fmt"

	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
)

type VideoChunkDB struct {
	modelStore
}

func NewVideoChunkStore(db *sql.DB, lock *sync.Mutex) repo.VideoChunkStore {
	return &VideoChunkDB{modelStore{db, lock}}
}

func (c *VideoChunkDB) Add(video *pb.VideoChunk) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	stm := `insert into video_chunks(id, chunk, address, startTime, endTime) values(?,?,?,?,?)`
	stmt, err := tx.Prepare(stm)
	if err != nil {
		log.Errorf("error in tx prepare: %s", err)
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(
		video.Id,
		video.Chunk,
		video.Address,
		video.StartTime,
		video.EndTime,
	)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (c *VideoChunkDB) ListByVideo(videoId string) []*pb.VideoChunk {
	c.lock.Lock()
	defer c.lock.Unlock()
	stm := "select * from video_chunks where id='" + videoId + "';"
	return c.handleQuery(stm)
}

func (c *VideoChunkDB) Get(videoId string, chunk string) *pb.VideoChunk {
	c.lock.Lock()
	defer c.lock.Unlock()
	stm := "select * from video_chunks where id='" + videoId + "' and chunk='" + chunk + "';"
    res := c.handleQuery(stm)
	if len(res) == 0 {
		return nil
	}
	return res[0]
}

func (c *VideoChunkDB) Delete(videoId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, err := c.db.Exec("delete from video_chunks where id=?", videoId)
	return err
}

func (c *VideoChunkDB) Find(videoId string, chunk string, startTime int32, endTime int32) []*pb.VideoChunk {
    if videoId == "" {
        return nil
    }
    if chunk == "" && startTime == -1 && endTime == -1 {
        return c.ListByVideo(videoId)
    }
    if chunk != "" {
	    stm := fmt.Sprintf("select * from video_chunks where id='%s' and chunk='%s'", videoId, chunk)
	    return c.handleQuery(stm)
    }
    if startTime == -1 {
	    stm := fmt.Sprintf("select * from video_chunks where id='%s' and endTime<=%d;", videoId, endTime)
        return c.handleQuery(stm)
    }
    if endTime == -1 {
	    stm := fmt.Sprintf("select * from video_chunks where id='%s' and startTime>=%d;", videoId, startTime)
        return c.handleQuery(stm)
    }
	stm := fmt.Sprintf("select * from video_chunks where id='%s' and startTime>=%d and endTime<=%d;", videoId, startTime, endTime)
    return c.handleQuery(stm)

    return nil
}

func (c *VideoChunkDB) handleQuery(stm string) []*pb.VideoChunk {
	var list []*pb.VideoChunk
	rows, err := c.db.Query(stm)
	if err != nil {
		log.Errorf("error in db query: %s", err)
		return nil
	}
	for rows.Next() {
		var id, chunk, address string
		var startTime, endTime int32
		if err := rows.Scan(&id, &chunk, &address, &startTime, &endTime); err != nil {
			log.Errorf("error in db scan: %s", err)
			continue
		}
		list = append(list, &pb.VideoChunk{
			Id:        id,
			Chunk:     chunk,
			Address:   address,
            StartTime: startTime,
            EndTime:   endTime,
		})
	}
	return list
}
