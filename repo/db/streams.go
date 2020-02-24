package db

import (
	"database/sql"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	"sync"
)

type StreamDB struct {
	modelStore
}

func (s *StreamDB) Add(stream *pb.Stream) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	stm := `insert or ignore into streams(id) values(?)`
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
	_, err = stmt.Exec(stream.Id)
	if err != nil {
		log.Error(err)
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()

}

func (s *StreamDB) Get(streamId string) *pb.Stream {
	s.lock.Lock()
	defer s.lock.Unlock()
	stm := "select * from streams where id='" + streamId + "';"
	res := s.handleQuery(stm)
	if len(res) == 0 {
		return nil
	}
	return res[0]
}

func (s *StreamDB) Delete(streamId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, err := s.db.Exec("delete from streams where id=?", streamId)
	return err
}

func (s *StreamDB) handleQuery(stm string) []*pb.Stream{
	var list []*pb.Stream
	rows, err := s.db.Query(stm)
	if err != nil {
		log.Errorf("error in db query: %s", err)
		return nil
	}
	for rows.Next(){
		var id string
		if err := rows.Scan(&id); err != nil{
			log.Errorf("error in db scan: %s", err)
			continue
		}
		list = append(list, &pb.Stream{
			Id: id,
		})
	}
	return list
}

func NewStreamStore(db *sql.DB, lock *sync.Mutex) repo.StreamStore {
	return &StreamDB{modelStore{db, lock}}
}