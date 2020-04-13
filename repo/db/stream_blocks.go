package db

import (
	"database/sql"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	"sync"
    "strconv"
)

type StreamBlockDB struct{
	modelStore
}

func (s StreamBlockDB) Add(streamblock *pb.StreamBlock) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	stm := `insert or ignore into stream_blocks(id, streamid, blockindex, blocksize, isroot, payload) values (?,?,?,?,?,?)`
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
	var isroot int
	if streamblock.IsRoot {
		isroot = 1
	}else {
		isroot = 0
	}
	_, err = stmt.Exec(streamblock.Id, streamblock.Streamid, streamblock.Index,streamblock.Size,isroot, streamblock.Description)
	if err != nil {
		_ = tx.Rollback()
		log.Error(err)
		return err
	}
	return tx.Commit()
}


func (s StreamBlockDB) ListByStream(streamid string, startindex int, maxnum int) []*pb.StreamBlock {
	s.lock.Lock()
	defer s.lock.Unlock()

	stm := "select * from stream_blocks where streamid='"  +streamid+  "' and blockindex >= " + strconv.Itoa(startindex)+  " order by blockindex limit "+strconv.Itoa(maxnum);

	res := s.handleQuery(stm)
	return res
}


func (s StreamBlockDB) LastIndex(streamid string) uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	stm := "select * from stream_blocks where streamid='"  +streamid+ "' order by blockindex";
	res := s.handleQuery(stm)

    id := uint64(0)

    for {
        if id >= uint64(len(res)) {
            break
        }
        if res[id].Index == id {
            id = id+1
        } else {
            break
        }
    }

	return id
}

//TODO: how many blocks do we have
func (s StreamBlockDB) BlockCount(streamid string) uint64{
	s.lock.Lock()
	defer s.lock.Unlock()

	stm := "select count(*) from stream_blocks where streamid='"  +streamid +"'" ;
	rows, err := s.db.Query(stm)
	if err != nil {
		log.Errorf("block count error: %s", err)
		return 0
	}
    var count uint64
    for rows.Next() {
	    err = rows.Scan(&count)
	    if err != nil {
		    log.Errorf("block count error: %s", err)
		    return 0
	    }
    }
    return count
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
		var id, streamid, payload string
		var index uint64
		var blocksize int32
		var isroot int
		err := rows.Scan(&id, &streamid, &index, &blocksize, &isroot, &payload)
		if err !=nil {
			log.Errorf("error in db scan: %s", err)
			continue
		}
		var isRootBool bool
		if isroot == 0{
			isRootBool=false
		}else{
			isRootBool=true
		}
		list = append(list, &pb.StreamBlock{
			Id: id,
			Streamid: streamid,
			Index: index,
			Size: blocksize,
			IsRoot: isRootBool,
            Description: payload,
		})
	}
	return list
}

func NewStreamBlockStore(db *sql.DB, lock *sync.Mutex) repo.StreamBlockStore {
	return &StreamBlockDB{modelStore{db,lock}}
}
