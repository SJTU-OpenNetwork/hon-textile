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
    //log.Debug("try get video lock")
	c.lock.Lock()
	defer c.lock.Unlock()
    //log.Debug("get video lock")
	
    tx, err := c.db.Begin()
	if err != nil {
		return err
	}
    stm := `insert or ignore into video_chunks(id, chunk, address, startTime, endTime, cid) values(?,?,?,?,?,?)`
	stmt, err := tx.Prepare(stm)
	if err != nil {
		log.Errorf("error in tx prepare: %s", err)
		return err
	}
    //log.Debug("after prepare")
	defer stmt.Close()
	_, err = stmt.Exec(
		video.Id,
		video.Chunk,
		video.Address,
		video.StartTime,
		video.EndTime,
		video.Index,
	)
    //log.Debug("after execute")
	if err != nil {
		_ = tx.Rollback()
        log.Error(err)
		return err
	}
    //log.Debug("out Add")
	return tx.Commit()
}

func (c *VideoChunkDB) ListByVideo(videoId string) []*pb.VideoChunk {
    //log.Debug("try get video lock")
	c.lock.Lock()
    //log.Debug("get video lock")
	defer c.lock.Unlock()
	stm := "select * from video_chunks where id='" + videoId + "';"
    //log.Debug("out List")
	return c.handleQuery(stm)
}

func (c *VideoChunkDB) Get(videoId string, chunk string) *pb.VideoChunk {
    //log.Debug("try get video lock")
	c.lock.Lock()
    //log.Debug("get video lock")
	defer c.lock.Unlock()
	stm := "select * from video_chunks where id='" + videoId + "' and chunk='" + chunk + "';"
	//log.Debugf("SQL: %s", stm)
    res := c.handleQuery(stm)
	if len(res) == 0 {
		return nil
	}
    //log.Debug("out GET")
	return res[0]
}

func (c *VideoChunkDB) GetByIndex(videoId string, index int64) *pb.VideoChunk{
	//log.Debugf("Get video by index %d", index)
	//log.Debug("try get video lock")
	c.lock.Lock()
	//log.Debug("get video lock")
	defer c.lock.Unlock()
	stm := fmt.Sprintf("select * from video_chunks where id='%s' and cid='%d'", videoId, index)
	//stm := "select * from video_chunks where id='" + videoId + "' and index='" + index + "';"
	//log.Debugf("SQL: %s", stm)
	res := c.handleQuery(stm)
	if len(res) == 0 {
		return nil
	}
	//log.Debug("out GET")
	return res[0]
}

func (c *VideoChunkDB) Delete(videoId string) error {
    //log.Debug("try get video lock")
	c.lock.Lock()
    //log.Debug("get video lock")
	defer c.lock.Unlock()
	_, err := c.db.Exec("delete from video_chunks where id=?", videoId)
	return err
}

func (c *VideoChunkDB) Find(videoId string, chunk string, startTime int64, endTime int64, index int64) []*pb.VideoChunk {
    //log.Debug("try get video lock")
	c.lock.Lock()
    //log.Debug("get video lock")
	defer c.lock.Unlock()

    //log.Debugf("VIDEOPIPELINE: Try to find chunk with query:\nvideoId: %s\nchunk: %s\nstartTime: %d\nendTime: %d\nindex: %d",
    //	videoId, chunk, startTime, endTime, index)
    if videoId == "" {
        return nil
    }
	if chunk != "" {
		//log.Debugf("VIDEOPIPELINE: Find video by chunk %s", chunk)
		stm := fmt.Sprintf("select * from video_chunks where id='%s' and chunk='%s'", videoId, chunk)
		//log.Debugf("SQL: %s", stm)
		return c.handleQuery(stm)
	}
	if index >= 0 {
		//log.Debugf("VIDEOPIPELINE: Find video by index %d", index)
		stm := fmt.Sprintf("select * from video_chunks where id='%s' and cid='%d'", videoId, index)
		//log.Debugf("SQL: %s", stm)
        result := c.handleQuery(stm)
        //log.Debug("finish query")
		return result
	}
    if startTime == -1 && endTime == -1 {
	    stm := "select * from video_chunks where id='" + videoId + "';"
	    return c.handleQuery(stm)
    }
    if startTime == -1 {
	    stm := fmt.Sprintf("select * from video_chunks where id='%s' and endTime<=%d;", videoId, endTime)
		//log.Debugf("SQL: %s", stm)
        return c.handleQuery(stm)
    }
    if endTime == -1 {
	    stm := fmt.Sprintf("select * from video_chunks where id='%s' and startTime>=%d;", videoId, startTime)
		//log.Debugf("SQL: %s", stm)
        return c.handleQuery(stm)
    }
	stm := fmt.Sprintf("select * from video_chunks where id='%s' and startTime>=%d and endTime<=%d;", videoId, startTime, endTime)
	//log.Debugf("SQL: %s", stm)
    return c.handleQuery(stm)
}

func (c *VideoChunkDB) handleQuery(stm string) []*pb.VideoChunk {
	var list []*pb.VideoChunk
	rows, err := c.db.Query(stm)
    //log.Debug("Query finish!")
	if err != nil {
		log.Errorf("error in db query: %s", err)
		return nil
	}
	for rows.Next() {
		var id, chunk, address string
		var startTime, endTime, cid int64

		if err := rows.Scan(&id, &chunk, &address, &startTime, &endTime, &cid); err != nil {
			log.Errorf("error in db scan: %s", err)
			continue
		}
		list = append(list, &pb.VideoChunk{
			Id:        id,
			Chunk:     chunk,
			Address:   address,
            StartTime: startTime,
            EndTime:   endTime,
            Index:       cid,
		})
	}
    //log.Debug("VideoChunkDB: handleQuery, got %d", len(list))
	return list
}
