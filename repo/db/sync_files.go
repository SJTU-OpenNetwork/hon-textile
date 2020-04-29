package db

import (
	"database/sql"
	"sync"
    "fmt"

	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	"github.com/SJTU-OpenNetwork/hon-textile/util"
)

type SyncFileDB struct {
	modelStore
}

func NewSyncFileStore(db *sql.DB, lock *sync.Mutex) repo.SyncFileStore {
	return &SyncFileDB{modelStore{db, lock}}
}

func (c *SyncFileDB) Add(file *pb.SyncFile) error {
	c.lock.Lock()
    defer c.lock.Unlock()
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

    stm := `insert or replace into sync_files(peer_address, file, type, date, operation) values(?,?,?,?,?)`
	stmt, err := tx.Prepare(stm)
	if err != nil {
		log.Errorf("error in tx prepare: %s", err)
		return err
	}
    log.Debug(stm)
	defer stmt.Close()
	_, err = stmt.Exec(
		file.PeerAddress,
        file.File,
		file.Type,
        util.ProtoNanos(file.Date),
        file.Operation,
	)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (c *SyncFileDB) ListByType(peerAddress string, fileType pb.SyncFile_Type) []*pb.SyncFile {
	c.lock.Lock()
	defer c.lock.Unlock()
    q := fmt.Sprintf("select * from sync_files where peer_address='%s' and type=%d order by date desc;", peerAddress, int32(fileType))
	return c.handleQuery(q)
}

func (c *SyncFileDB) Delete(file *pb.SyncFile) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, err := c.db.Exec("delete from videos where peer_address=? and file=? and type=", file.PeerAddress, file.File, file.Type)
	return err
}

func (c *SyncFileDB) handleQuery(stm string) []*pb.SyncFile {
	var list []*pb.SyncFile
	rows, err := c.db.Query(stm)
	if err != nil {
		log.Errorf("error in db query: %s", err)
		return nil
	}
	for rows.Next() {
		var address, file string
		var fileType, operation int32
        var date int64
		if err := rows.Scan(&address, &file, &fileType, &date, &operation); err != nil {
			log.Errorf("error in db scan: %s", err)
			continue
		}
		list = append(list, &pb.SyncFile{
			PeerAddress: address,
            File: file,
            Type: pb.SyncFile_Type(fileType),
            Date: util.ProtoTs(date),
            Operation: pb.SyncFile_Operation(operation),
		})
	}
	return list
}
