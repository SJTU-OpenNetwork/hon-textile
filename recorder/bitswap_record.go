package recorder

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

// Manage records from bitswap.
// Functionality:
//		- Handle record thrown from bitswap.
//		- Maintain records belonging to the same task. Sending a file for example.
//		- Save it to disk if necessary.
//		- Send records to application layer through notifications if necessary.
//		- Send records to application layer through some function directly if necessary.
//		- Flush memory regularly (or use a limited cache size).
// Note:
//		- For now, records from bitswap would not be sent to other peers.
//			But we may do such thing in future...maybe.
//			Write such functionality in this file if so.
// event type is saved in event.go
//var a := time.Now()

var bitswapRecordCh = make(chan *BitswapRecord, 100)	// work as a cache above store
const millovernano = int64(time.Millisecond)/int64(time.Nanosecond)

type BitswapRecord struct {
	Event string	`json:"event"`
	Date int64  	`json:"date"`
	Cid string 		`json:"cid"`
	Info map[string]string `json:"info"`
}

type BitswapRecordStore struct {
	cache *list.List
	capacity int
	lock sync.Mutex
}

// transfer nano second to mill second.
func nanoToMill(nano int64) int64 {
	return nano / millovernano
}

func newBitswapRecordStore(cap int) *BitswapRecordStore {
	return &BitswapRecordStore{
		cache:    list.New(),
		capacity: cap,
	}
}

func (store *BitswapRecordStore) add(r *BitswapRecord) error {
	store.lock.Lock()
	defer store.lock.Unlock()
	store.cache.PushBack(r)
	if store.cache.Len() > store.capacity {
		store.cache.Remove(store.cache.Front())
	}
	return nil
}

func (store *BitswapRecordStore) filterCid(cid string) []*BitswapRecord {
	store.lock.Lock()
	defer store.lock.Unlock()
	res := make([]*BitswapRecord, 0)
	for e:= store.cache.Front(); e!= nil; e = e.Next() {
		record := e.Value.(*BitswapRecord)
		if record.Cid == cid {

		}
		res = append(res, e.Value.(*BitswapRecord))
	}
	return res
}



func AddBitswapRecord(event string, cid string, info map[string]string) error {
	if !Online {
		return errors.New("recorder not online")
	}
	bitswapRecordCh <- &BitswapRecord{
		Event: event,
		Date:  nanoToMill(time.Now().UnixNano()),
		Cid: cid,
		Info:  info,
	}
	return nil
}

func (store *BitswapRecordStore)listenCh() {
	for {
		select {
		case r := <- bitswapRecordCh:
			log.Debugf("Record from bitswap with event: %s", r.Event)
			err := store.add(r)
			if err != nil {
				log.Error(err)
			}
		}
	}
}