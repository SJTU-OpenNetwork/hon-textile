package stream

import (
	//"context"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
)


// providerStore is used to manage stream providers.
// It should be thread-safe with both store, retrieve, de-duplication method.
// Note:
//		Each substream only need one Provider.
//		One Provider may provide several substreams belonging to different streams.
type providerStore struct {
    providers map[string] *Provider
    potentialProviders map[string] *Provider
	//ctx context.Context
	lock sync.Mutex
}

func newProviderStore() *providerStore {
	return &providerStore{
		providers:make(map[string] *Provider),
		potentialProviders: make(map[string] *Provider),
	}
}

func (ps *providerStore) getOrCreate(pid string) *Provider {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	p, ok := ps.providers[pid]
	if !ok {
		p = newProvider(pid)
		ps.providers[pid] = p
	}
	return p
}

// add a Provider
// do not support substream now
// [deprecated] use getOrCreate() and Provider.add() instead.
/*
func (ps *providerStore) add(pid string, substream *providedSubstream) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	p, ok := ps.providers[pid]
	if !ok {
		p = newProvider(pid)
		ps.providers[pid] = p
	}
	p.add(substream)

	return nil
}
*/

// peerDisconnected is called by the upper manager
// return a list of streams that need to resubscribe
func (ps *providerStore) peerDisconnected(pid peer.ID) (map[string] *pb.StreamRequest, error) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	return nil, nil
}

// do not support substream currently
func (ps *providerStore) GetProvider(sid string) peer.ID {
    ps.lock.Lock()
    defer ps.lock.Unlock()

    for pid, item := range ps.providers {
        for _, stream := range item.subStreams {
            if stream.streamId == sid {
                return peer.ID(pid)
            }
        }
    }
    return peer.ID("")
}

// do not support substream currently
func (ps *providerStore) RemoveStream(sid string) peer.ID {
    ps.lock.Lock()
    defer ps.lock.Unlock()

    for pid, item := range ps.providers {
        id := -1
        for index, stream := range item.subStreams {
            if stream.streamId == sid {
                id = index
            }
        }
        if id > -1 {
            temp := append(item.subStreams[:id],item.subStreams[id+1:]...)
            item.subStreams = temp
            return peer.ID(pid)
        }
    }
    return peer.ID("")
}
