package stream

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
)


type Provider struct {
    pid peer.ID
    config *pb.StreamRequest
}

// providerStore is used to manage stream providers.
// It should be thread-safe with both store, retrieve, de-duplication method.
type providerStore struct {
	currentProviderList map[string] []*Provider
    providerIndex map[peer.ID] *Provider // for quic search
	lock sync.Mutex
}

func newProviderStore() *providerStore {
	return &providerStore{
		currentProviderList: make(map[string][]*Provider),
		providerIndex: make(map[peer.ID] *Provider),
	}

}

// add a provider
// do not support substream now
func (ps *providerStore) add(pid peer.ID, req *pb.StreamRequest) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

    provider := &Provider{
        pid: pid,
        config: req,
    }
	ps.currentProviderList[req.Id] = append(ps.currentProviderList[req.Id], provider)
    ps.providerIndex[provider.pid] = provider
	return nil
}

// peerDisconnected is called by the upper manager
func (ps *providerStore) peerDisconnected(pid peer.ID) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()
   
    provider := ps.providerIndex[pid]
    if provider == nil {
        return nil
    }

    currentProviderList[provider.config.Id][0] = nil
    ps.providerIndex[pid] = nil
}
