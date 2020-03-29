package stream

import (
	"sync"

	//"github.com/libp2p/go-libp2p-core/peer"
)

// lostIndex is used to detect the lost indexes.
// 	- previous: used to record the last received index.
//	- interval: interval between two adjacent index. equal to numSubstream.
//	- lostFunc: function called when a lost index detected.
// DO NOT USE LOSTINDEX ALONE. USE IT INSIDE PROVIDEDSUBSTREAM.
type lostIndex struct {
	previous uint64
	interval uint64
	lostFunc func(lostIndex uint64)
}

func (l *lostIndex) add(streamInd uint64) {
	if (streamInd - l.previous > l.interval){
		currentInd := l.previous + l.interval
		for {
			//l.out <- currentInd
			if (streamInd - currentInd < l.interval){
				break
			}
			l.lostFunc(currentInd)
			currentInd += l.interval
		}
	}
	l.previous = streamInd
}

// Core of Provider.
type providedSubstream struct {
	// Value assigned when initializing.
	streamId string
	streamMap uint64
	providerId string
	numSubstream uint64
	startIndex uint64
	//out chan *lostReport
	handleLost func(*lostReport)

	// Value Created.
	lost *lostIndex
	previous uint64
	//ctx context.Context
}

// A report used to output the index lost event.
type lostReport struct {
	streamId string
	streamMap uint64
	index uint64
	providerId string
}

func (report *lostReport) Loggable() map[string]interface{}{
	return map[string]interface{}{
		"providerId": report.providerId,
		"streamId": report.streamId,
		"streamMap": report.streamMap,
		"index": report.index,
	}
}

func (ps *providedSubstream) indexLost(lostIndex uint64){
	report := &lostReport{
		streamId: ps.streamId,
		streamMap: ps.streamMap,
		providerId: ps.providerId,
		index: lostIndex,
	}
	//ps.out <- report
	ps.handleLost(report)
}

// TODO: Find a better way to initialize provided substream.
func newProvidedSubstream(streamId string, streamMap uint64, numSubstream uint64, startIndex uint64, providerId string, lostFunc func(report *lostReport)) *providedSubstream {
	res := &providedSubstream{
		streamId:   streamId,
		streamMap:  streamMap,
		numSubstream:numSubstream,
		startIndex: startIndex,
		providerId: providerId,
		handleLost: lostFunc,
		lost:  &lostIndex{
			previous:   startIndex - numSubstream,
			interval:   numSubstream,
		},
	}
	res.lost.lostFunc = res.indexLost
	return res
}


// One Provider represents one ipfs peer.
// Function:
//	- Record the information about all substreams provided by one peer.
//	- Detect the lose of blocks.
//	- Find out which substream should be re-request if a peer is disconnect.
//		Further handle Provider disconnect
// Note:
// Provider can be used from outside stream package.
type Provider struct {
	pid string
	subStreams []*providedSubstream
	lock sync.Mutex
}

func newProvider(pid string) *Provider {
	return &Provider{
		pid:     pid,
		subStreams: make([]*providedSubstream,0),
	}
}

func (p *Provider) add (sub *providedSubstream) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.subStreams = append(p.subStreams, sub)
}
