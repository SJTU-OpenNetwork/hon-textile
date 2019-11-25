package db

import (
	"database/sql"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/util"
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
		PeerAddress: "Peerid",
		File: "Hash",
		Type: pb.SyncFile_Type(0),
		Date: util.ProtoTs(0),
		Operation: pb.SyncFile_Operation(0),
	})
	if(err!=nil){
		t.Error(err)
	}
	//syncFileStore
}


func TestSyncFileDB_List(t *testing.T) {
}

