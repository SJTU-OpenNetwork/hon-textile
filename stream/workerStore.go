package stream

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
)

// workerStore is used to manage worker storage within a streamManager.
// It should be thread-safe with both store, retrieve, de-duplication method.
type workerStore struct {
	workerList map[string] []*streamWorker
	lock sync.Mutex
}

func newWorkerStore() *workerStore {
	return &workerStore{
		workerList: make(map[string][]*streamWorker),
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
