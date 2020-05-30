package stream

import (
	"encoding/json"
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
)

// workerStore is used to manage worker storage within a streamManager.
// It should be thread-safe with both store, retrieve, de-duplication method.
type workerStore struct {
	// workerList:
	//		map[streamId] list of workers
	//		We need to notice workers by streamId. So we use streamId as key.
	workerList map[string] []*streamWorker
	lock sync.Mutex
    load int
}

func newWorkerStore() *workerStore {
	return &workerStore{
		workerList: make(map[string][]*streamWorker),
        load: 0,
	}

}

func (ws *workerStore) isRedundant(pid peer.ID, req *pb.StreamRequest) bool {
	// Two worker is same if they have the same:
	//		- destination peer.
	//		- streamId
	//		- overlapped substream
	ws.lock.Lock()
	defer ws.lock.Unlock()


	return false
}

// add a worker into worker store
// Note:
//		add method would not judge whether the worker is redundant
func (ws *workerStore) add(worker *streamWorker) error {
	ws.lock.Lock()
	defer ws.lock.Unlock()

	// Note: In golang, we can append data in a nil slice directly.
	ws.workerList[worker.req.Id] = append(ws.workerList[worker.req.Id], worker)
	return nil
}

// newFileAdd send work signal to workers in workerStore
func (ws *workerStore) newFileAdd(streamId string) error {
	ws.lock.Lock()
	defer ws.lock.Unlock()

	tmplist, ok := ws.workerList[streamId]
	if ok {
		for _, w := range tmplist {
			w.notice()
		}
	}
	// TODO
	//		Raise an error if there is no worker with streamId
	return nil
}

func (ws *workerStore) Workload() int {
	ws.lock.Lock()
	defer ws.lock.Unlock()

    load := 0
    for _, tmplist := range(ws.workerList){
        for _, w := range tmplist {
            if !w.end {
                load ++
            }
        }
    
    }
    return load
}

// end workers of specific streamId
func (ws *workerStore) endStream(streamId string) {
	ws.lock.Lock()
	defer ws.lock.Unlock()
	tmpList, ok := ws.workerList[streamId]
	if ok {
		for _, w := range tmpList {
			log.Debugf("[%s] Stream %s, Peer %s", TAG_WORKERSTORE_REMOVE, streamId, w.pid)
			w.cancel()
		}
		delete(ws.workerList, streamId)
	}
}

// end workers of specific streamId and peerId
func (ws *workerStore) endWorker(streamId string, peerId string) {
	ws.lock.Lock()
	defer ws.lock.Unlock()
	tmpList, ok := ws.workerList[streamId]
	newList := make([]*streamWorker, 0, len(tmpList))
	if ok {
		for _, w := range tmpList {
			if w.pid.Pretty() == peerId {
				w.cancel()
				log.Debugf("[%s] Stream %s, Peer %s", TAG_WORKERSTORE_REMOVE, streamId, w.pid)
			} else if w.end {
                continue
            }else {
				// add the remaining workers to newList
				newList = append(newList, w)
			}
		}
		ws.workerList[streamId] = newList
		if len(newList) == 0 {
		    delete(ws.workerList, streamId)
        }
	}
}

// end workers of specific peerId
func (ws *workerStore) endPeer(pid string) {
	ws.lock.Lock()
	defer ws.lock.Unlock()
	// TODO: Find a better way to index workers of specific peerId instead of traverse the whole map.
	deleteList := make([]string,0) // Delete list contains the streamId that has no workers after traverse.
	for streamId, tmpList := range ws.workerList {
		newList := make([]*streamWorker, 0, len(tmpList))
		for _, worker := range(tmpList) {
			if worker.pid.Pretty() == pid {
				worker.cancel()
				log.Debugf("[%s] Stream %s, Peer %s", TAG_WORKERSTORE_REMOVE, streamId, worker.pid)
			} else {
				// add the remaining workers to newList
				newList = append(newList, worker)
			}
		}
		ws.workerList[streamId] = newList
		if len(newList) == 0 {
			deleteList = append(deleteList, streamId)
		}
	}

	for _, k := range deleteList {
		delete(ws.workerList, k)
	}
}

func (ws *workerStore) Loggable() map[string]interface{} {
	ws.lock.Lock()
	defer ws.lock.Unlock()
	workers := make(map[string][]string)
	for streamId, tmpList := range ws.workerList {
		tmpSlice := make([]string, 0, len(tmpList))
		for _, worker := range tmpList {
			tmpSlice = append(tmpSlice, worker.pid.Pretty())
		}
		workers[streamId] = tmpSlice
	}
	return map[string]interface{}{
		"Number of Workers" : ws.load,
		"Workers": workers,
	}
}

func (ws *workerStore) PrintOut() {
	infos :=  ws.Loggable()
	js, err := json.MarshalIndent(infos, "", "  ")
	if err != nil {
		fmt.Printf("%s\n", err.Error())
	} else {
		fmt.Printf("%s\n", string(js))
	}
}
