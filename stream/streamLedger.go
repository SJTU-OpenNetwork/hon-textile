package stream

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
	"time"
)

// streamledger store stream exchange relationship between two peers.
type  streamLedger struct {
	// Patner is the remote peer.
 	Partner peer.ID

 	// lastExchange is the time of the last stream exchange.
	lastExchange time.Time

 	// exchangeCount is the number of stream exchange.
	exchangeCount uint64

 	// wantList store the wanted stream of the remote peer.
	wantList *streamWantlist

 	// haveList store the stream of remote peer has.
	haveList *streamHavelist

	lk sync.Mutex
}

// newStreamLedger new a ledger for a peer.
func newStreamLedger(p peer.ID) *streamLedger{
	return &streamLedger{
		Partner: p,
		wantList: nil,
		haveList: nil,
	}
}

func (l *streamLedger) Wants(streamid string, priority int) {
	l.wantList.Add(streamid,priority)
}

func (l *streamLedger) CancelWant(streamid string) {
	l.wantList.Remove(streamid)
}

func (l *streamLedger) Haves(streamid string, priority int) {
	l.wantList.Add(streamid,priority)
}

func (l *streamLedger) CancelHave(streamid string) {
	l.wantList.Remove(streamid)
}