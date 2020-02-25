package db

import (
	"database/sql"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	"sync"
)

type StreamBlockDB struct{
	modelStore
}

func (s StreamBlockDB) Add(streamblock *pb.StreamBlock) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	stm := `insert or ignore int streams(id, streamid, blockindex) values (?,?,?)`
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(stm)
	if err != nil {
		log.Errorf("error in tx prepare: %s", err)
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(streamblock.Id, streamblock.Streamid, streamblock.Index)
	if err != nil {
		_ = tx.Rollback()
		log.Error(err)
		return err
	}
	return tx.Commit()
}

func (s StreamBlockDB) ListByStream(streamid string) []*pb.StreamBlock {
	s.lock.Lock()
	defer s.lock.Unlock()

	stm := "select * from stream_blocks where streamid='"+streamid+"';"
	return s.handleQuery(stm)
}

func (s StreamBlockDB) GetByCid(cid string) *pb.StreamBlock {
	s.lock.Lock()
	defer s.lock.Unlock()

	stm := "select * from stream_blocks where id='"+cid+"';"
    res := s.handleQuery(stm)
	if len(res) == 0 {
		return nil
	}
    //log.Debug("out GET")
	return res[0]
}

func (s StreamBlockDB) Delete(streamid string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, err := s.db.Exec("delete from stream_blocks where id=?",streamid)
	return err
}

func (s *StreamBlockDB) handleQuery(stm string) []*pb.StreamBlock {
	var list []*pb.StreamBlock
	rows, err := s.db.Query(stm)
	if err != nil {
		log.Errorf("error in db query: %s", err)
		return nil
	}
	for rows.Next(){
		var id, streamid string
		var index uint64
		err := rows.Scan(&id, &streamid, &index)
		if err !=nil {
			log.Errorf("error in db scan: %s", err)
			continue
		}
		list = append(list, &pb.StreamBlock{
			Id: id,
			Streamid: streamid,
			Index: index,
		})
	}
	return list
}

func NewStreamBlockStore(db *sql.DB, lock *sync.Mutex) repo.StreamBlockStore {
	return &StreamBlockDB{modelStore{db,lock}}
}
