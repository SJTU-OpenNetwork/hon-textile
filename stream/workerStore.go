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
	return nil
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

func (ws *workerStore) add(worker *streamWorker) error {
	ws.lock.Lock()
	defer ws.lock.Unlock()
	return nil
}

func (ws *workerStore) signalWorkers(signal *workerSignal) error {
	ws.lock.Lock()
	defer ws.lock.Unlock()

	return nil
}