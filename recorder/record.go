package recorder

import (
	"sync"
	"time"
)

type Record struct {
	key string
	createTime time.Time
}

func newRecord() *Record {
	return &Record{createTime:time.Now()}
}

// RecordStore store all the records indexed by unique key
type RecordStore struct {
	store map[string]*Record
	lock sync.Mutex
}

//func (rs *RecordStore) addReport()
