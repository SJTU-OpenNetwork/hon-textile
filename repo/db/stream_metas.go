package db

import (
	"database/sql"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	"sync"
)

type StreamMetaDB struct {
	modelStore
}

// TODO: add nblocks in database
func (s *StreamMetaDB) Add(streammeta *pb.StreamMeta) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	stm := `insert or ignore into stream_metas(id, nstream, bitrate, caption, nblocks, posterid) values(?,?,?,?,?,?)`
	tx, err := s.db.Begin()
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
	_, err = stmt.Exec(streammeta.Id, streammeta.Nsubstreams, streammeta.Bitrate, streammeta.Caption, streammeta.Nblocks, streammeta.Posterid)
	if err != nil {
		log.Error(err)
		_ = tx.Rollback()
		return err
	}
	log.Debugf("insert successfully : %s ",streammeta.Id)
	return tx.Commit()
}

func (c *StreamMetaDB) UpdateNblocks(id string, nblocks uint64) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, err := c.db.Exec("update stream_metas set nblocks=? where id=?", nblocks, id)
	return err
}

func (s *StreamMetaDB) Get(streamId string) *pb.StreamMeta {
	s.lock.Lock()
	defer s.lock.Unlock()
	stm := "select * from stream_metas where id='" + streamId + "';"
	res := s.handleQuery(stm)
	if len(res.Items) == 0 {
		return nil
	}
	return res.Items[0]
}

func (s *StreamMetaDB) List() *pb.StreamMetaList {
	s.lock.Lock()
	defer s.lock.Unlock()
	stm := "select * from stream_metas;"
	res := s.handleQuery(stm)
	return res
}

func (s *StreamMetaDB) Delete(streamId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, err := s.db.Exec("delete from stream_metas where id=?", streamId)
	return err
}

func (s *StreamMetaDB) handleQuery(stm string) *pb.StreamMetaList{
	//var list []*pb.StreamMeta
	var list = &pb.StreamMetaList{
		Items:make([]*pb.StreamMeta, 0),
	}
	rows, err := s.db.Query(stm)
	if err != nil {
		log.Errorf("error in db query: %s", err)
		return nil
	}
	for rows.Next(){
		var id, caption string
        var nstream, bitrate int32
        var nblocks uint64
		var posterid string
		if err := rows.Scan(&id, &nstream, &bitrate, &caption, &nblocks, &posterid); err != nil{
			log.Errorf("error in db scan: %s", err)
			continue
		}
		list.Items = append(list.Items, &pb.StreamMeta{
			Id: id,
            Nsubstreams: nstream,
            Bitrate: bitrate,
            Caption: caption,
            Nblocks: nblocks,
            Posterid: posterid,
		})
	}
	return list
}

func NewStreamMetaStore(db *sql.DB, lock *sync.Mutex) repo.StreamMetaStore {
	return &StreamMetaDB{modelStore{db, lock}}
}
