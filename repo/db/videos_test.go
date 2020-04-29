package db

import (
	"database/sql"
	"sync"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
)

var videoStore repo.VideoStore

func init() {
	setupVideoDB()
 }

func setupVideoDB() {
	conn, _ := sql.Open("sqlite3", ":memory:")
	initDatabaseTables(conn, "")
	videoStore = NewVideoStore(conn, new(sync.Mutex))
}

func TestVideoDB_Add(t *testing.T) {
	err := videoStore.Add(&pb.Video{
		Id:       "abc",
        Caption:  "Test",
        VideoLength: 1000,
        Poster:    "123",
	})
	if err != nil {
		t.Error(err)
	}
	stmt, err := videoStore.PrepareQuery("select id from videos where id=?")
	defer stmt.Close()
	var id string
	err = stmt.QueryRow("abc").Scan(&id)
	if err != nil {
		t.Error(err)
	}
	if id != "abc" {
		t.Errorf(`expected id "abc" got %s`, id)
	}
}

func TestVideoDB_Get(t *testing.T) {
	err := videoStore.Add(&pb.Video{
		Id:       ksuid.New().String(),
        Caption:  "Test",
        VideoLength: 1000,
        Poster:    ksuid.New().String(),
	})
	if err != nil {
		t.Error(err)
	}
	err = videoStore.Add(&pb.Video{
		Id:       "boo",
        Caption:  "Testboo",
        VideoLength: 1000,
        Poster:    ksuid.New().String(),
	})
	filtered := videoStore.Get("boo")
	if filtered.Caption != "Testboo" {
		t.Error("returned incorrect number of peers")
		return
	}
}

func TestVideoDB_Delete(t *testing.T) {
	err := videoStore.Add(&pb.Video{
		Id:       "foo",
        Caption:  "Test",
        VideoLength: 1000,
        Poster:    ksuid.New().String(),
	})
	if err != nil {
		t.Error(err)
	}
	err = videoStore.Delete("foo")
	if err != nil {
		t.Error(err)
	}
	stmt, err := videoStore.PrepareQuery("select id from thread_peers where id=?")
	defer stmt.Close()
	var id string
	err = stmt.QueryRow("foo").Scan(&id)
	if err == nil {
		t.Error("delete failed")
	}
}

