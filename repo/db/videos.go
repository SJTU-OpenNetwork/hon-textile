package db

import (
	"database/sql"
	"sync"

	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
)

type VideoDB struct {
	modelStore
}

func NewVideoStore(db *sql.DB, lock *sync.Mutex) repo.VideoStore {
	return &VideoDB{modelStore{db, lock}}
}

func (c *VideoDB) Add(video *pb.Video) error {
	c.lock.Lock()
	log.Debug("Get Lock")
    defer c.lock.Unlock()

	log.Debug("After Search")
    stm := `insert or ignore into videos(id, caption, videoLength, poster, width, height, rotation) values(?,?,?,?,?,?,?)`
	tx, err := c.db.Begin()
	if err != nil {
        log.Error(err)
		return err
	}
	stmt, err := tx.Prepare(stm)
	if err != nil {
		log.Errorf("error in tx prepare: %s", err)
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(
		video.Id,
        video.Caption,
		video.VideoLength,
        video.Poster,
        video.Width,
        video.Height,
        video.Rotation,
	)
	if err != nil {
        log.Error(err)
		_ = tx.Rollback()
		return err
	}
    log.Debug("out add")
	return tx.Commit()
}

func (c *VideoDB) Get(videoId string) *pb.Video {
	c.lock.Lock()
	defer c.lock.Unlock()
	stm := "select * from videos where id='" + videoId + "';"
    res := c.handleQuery(stm)
	if len(res) == 0 {
		return nil
	}
	return res[0]
}

func (c *VideoDB) Delete(videoId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, err := c.db.Exec("delete from videos where id=?", videoId)
	return err
}

func (c *VideoDB) handleQuery(stm string) []*pb.Video {
	var list []*pb.Video
	rows, err := c.db.Query(stm)
	if err != nil {
		log.Errorf("error in db query: %s", err)
		return nil
	}
	for rows.Next() {
		var id, caption, poster string
		var videoLength int64
		var width, height, rotation int32
		if err := rows.Scan(&id, &caption, &videoLength, &poster, &width, &height, &rotation); err != nil {
			log.Errorf("error in db scan: %s", err)
			continue
		}
		list = append(list, &pb.Video{
			Id:        id,
            Caption:   caption,
            VideoLength: videoLength,
            Poster:      poster,
            Width:		width,
            Height:		height,
            Rotation:	rotation,
		})
	}
	return list
}
