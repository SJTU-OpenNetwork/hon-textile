package db

import (
	"database/sql"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"sync"

	"testing"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
)

var syncFileStore repo.SyncFileStore

func init() {
	setupSyncFileDB()
}

func setupSyncFileDB() {
	conn, _ := sql.Open("sqlite3", ":memory:")
	initDatabaseTables(conn, "")
	syncFileStore = NewSyncFileStore(conn, new(sync.Mutex))
}

func TestSyncFileDB_Add(t *testing.T) {
	err := syncFileStore.Add(&pb.SyncFile{
	})
	if(err!=nil){
		t.Error(err)
	}
	//syncFileStore
}


func TestSyncFileDB_List(t *testing.T) {
}

