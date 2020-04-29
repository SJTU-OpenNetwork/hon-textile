package migrations

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func initAt017(db *sql.DB, pin string) error {
	var sqlStmt string
	if pin != "" {
		sqlStmt = "PRAGMA key = '" + pin + "';"
	}

	sqlStmt += `
    create table if not exists thread_peers (id text not null, threadId text not null, welcomed integer not null);
    create table if not exists threads (id text not null, initiator integer);
    insert into threads (id, initiator) values (1,1001);
    insert into thread_peers (id, threadId, welcomed) values (1001, 1, 0);
    insert into thread_peers (id, threadId, welcomed) values (1002, 1, 0);
    `
    _, err := db.Exec(sqlStmt)
	if err != nil {
		return err
	}
	return nil
}

func Test018(t *testing.T) {
	var dbPath string
	_ = os.Mkdir("./datastore", os.ModePerm)
	dbPath = path.Join("./", "datastore", "mainnet.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Error(err)
		return
	}
	if err := initAt017(db, ""); err != nil {
		t.Error(err)
		return
	}

	// go up
	var m Minor018
	if err := m.Up("./", "", false); err != nil {
		t.Error(err)
		return
	}

	// test new tables
    admins, err := db.Query("select id from thread_peers where admin=1")
	if err != nil {
		t.Error(err)
		return
	}
    var id string
    for admins.Next(){
        _ = admins.Scan(&id)
        t.Log(id)
    }

	// ensure that version file was updated
	version, err := ioutil.ReadFile("./repover")
	if err != nil {
		t.Error(err)
		return
	}
	if string(version) != "19" {
		t.Error("failed to write new repo version")
		return
	}

	if err := m.Down("./", "", false); err != nil {
		t.Error(err)
		return
	}
	_ = os.RemoveAll("./datastore")
	_ = os.RemoveAll("./repover")
}
