package db

import (
	"database/sql"
	"sync"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
)

var SyncFileStore repo.SyncFileStore

func init() {
	setupSyncFileDB()
}

func setupSyncFileDB() {
	conn, _ := sql.Open("sqlite3", ":memory:")
	initDatabaseTables(conn, "")
	syncFileStore = NewSyncFileStore(conn, new(sync.Mutex))
}

func TestSyncFileDB_Add(t *testing.T) {
}


func TestSyncFileDB_List(t *testing.T) {
}

