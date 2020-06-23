package stream

import (
	"container/heap"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/util"
	"sync"

	//"github.com/libp2p/go-libp2p-core/peer"
)

type providedStream struct {
	streamId string
	providerId string
	startIndex uint64
	nextIndex uint64
	//currentIndex uint64
	blocks util.Heap
	hLock sync.Mutex
}

// *streamBlock implements util.HeapItem
type streamBlock struct {
	block *pb.StreamBlock
}
func (b *streamBlock) Less(b2 util.HeapItem) bool {
	return b.block.Index < b2.(*streamBlock).block.Index
}

// add a streamBlock to providedStream
// return all the completed root block
// Note:
//	- Push the block to ipfs node before calling this function
func (s *providedStream) addBlock(b *pb.StreamBlock) []*pb.StreamBlock {
	s.hLock.Lock()
	defer s.hLock.Unlock()
	res := make([]*pb.StreamBlock, 0)
	heap.Push(&s.blocks, &streamBlock{block: b})
	for !s.blocks.IsEmpty() && s.blocks.Top().(*streamBlock).block.Index == s.nextIndex {
		s.nextIndex = s.nextIndex + 1
		tBlock := heap.Pop(&s.blocks).(*streamBlock).block
		if tBlock.IsRoot {
			res = append(res, tBlock)
		}
	}
	return res
}


// Access through ProvidedStreams only.
type ProvidedStreams struct {
	streams []*providedStream
	lock sync.Mutex
}

func (p *ProvidedStreams) getOrCreate(streamId string, providerId string, startIndex uint64) *providedStream {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, s := range p.streams {
		if s.streamId == streamId {
			return s
		}
	}
	newStream := &providedStream{
		streamId:   streamId,
		providerId: providerId,
		startIndex: startIndex,
		nextIndex:  startIndex,
		blocks:     make(util.Heap,0,10),
	}
	p.streams = append(p.streams, newStream)
	return newStream
}

func (p *ProvidedStreams) remove(streamId string) *providedStream {
	p.lock.Lock()
	defer p.lock.Unlock()
	newStreams := make([]*providedStream, 0, len(p.streams))
	var result *providedStream
	result = nil
	for _, s := range p.streams {
		if s.streamId != streamId {
			newStreams = append(newStreams, s)
		} else {
			result = s
		}
	}
	p.streams = newStreams
	return result
}

// Get streams provided by providerId
func (p *ProvidedStreams) providedBy(providerId string) []*providedStream {
	p.lock.Lock()
	defer p.lock.Unlock()
	var res []*providedStream
	res = nil
	for _, s := range p.streams {
		if s.providerId == providerId {
			res = append(res, s)
		}
	}
	return res
}

func (p *ProvidedStreams) getProvider(streamId string) string {
	for _, s := range p.streams {
		if s.streamId == streamId {
			return s.providerId
		}
	}
	return ""
}
