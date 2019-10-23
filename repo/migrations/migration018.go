package migrations

import (
	"database/sql"
	"os"
	"path"

	_ "github.com/mutecomm/go-sqlcipher"
)

type Minor018 struct{}

/**
  * @author Jerry
  * parsed test at 2019-10-23
  */
func (Minor018) Up(repoPath string, pinCode string, testnet bool) error {
	var dbPath string
	if testnet {
		dbPath = path.Join(repoPath, "datastore", "testnet.db")
	} else {
		dbPath = path.Join(repoPath, "datastore", "mainnet.db")
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	if pinCode != "" {
		if _, err := db.Exec("pragma key='" + pinCode + "';"); err != nil {
			return err
		}
	}

    // add column admin
	query := `
    alter table thread_peers add column admin integer not null default 0;
    `
	if _, err := db.Exec(query); err != nil {
		return err
	}

    // set thread initiator to admin
    query = `update thread_peers set admin=1 where id in (select initiator from threads where id=threadId)`
	if _, err := db.Exec(query); err != nil {
		return err
	}

	// update version
	f19, err := os.Create(path.Join(repoPath, "repover"))
	if err != nil {
		return err
	}
	defer f19.Close()
	if _, err = f19.Write([]byte("19")); err != nil {
		return err
	}
	return nil
}

func (Minor018) Down(repoPath string, pinCode string, testnet bool) error {
	return nil
}

func (Minor018) Major() bool {
	return false
}
