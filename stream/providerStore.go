package stream

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
)


type Provider struct {
    pid peer.ID
    streams []*pb.StreamRequest
}

// providerStore is used to manage stream providers.
// It should be thread-safe with both store, retrieve, de-duplication method.
// Note:
//		Each substream only need one provider.
//		One provider may provide several substreams belonging to different streams.
type providerStore struct {
	currentProviderList map[string] []*Provider	// map[streamId] []Provider
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

    provider, ok := ps.providerIndex[pid]
    if !ok {
        provider = &Provider{
            pid: pid,
            streams: make([]*pb.StreamRequest,0),
        }
		ps.currentProviderList[req.Id] = append(ps.currentProviderList[req.Id], provider)
		ps.providerIndex[provider.pid] = provider
    }
    provider.streams = append(provider.streams, req)

	return nil
}

// peerDisconnected is called by the upper manager
// return a list of streams that need to resubscribe
func (ps *providerStore) peerDisconnected(pid peer.ID) ([] *pb.StreamRequest, error) {
	ps.lock.Lock()
	defer ps.lock.Unlock()
   
    provider, ok := ps.providerIndex[pid]
    if !ok {
        return nil, nil	// Note that this func may return a legal nil.
    }

    //ps.currentProviderList[provider.config.Id][0] = nil
    for _, stream := range provider.streams{
		streamId := stream.Id
		proverderList := ps.currentProviderList[streamId]
		newList := make([]*Provider, 0, len(proverderList))
		for _, p := range proverderList{
			if p.pid.Pretty() != stream.Id {
				newList = append(newList, p)
			}
		}
		ps.currentProviderList[streamId] = newList
	}

    ps.providerIndex[pid] = nil
    return provider.streams, nil
}

func (ps *providerStore) getProvider(config *pb.StreamRequest) *Provider {
    // do not support substream for now
    providers, ok := ps.currentProviderList[config.Id]
    if !ok {
        return nil
    } else {
    	return providers[0]
	}
}
