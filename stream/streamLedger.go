package stream

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
	"time"
)

//streamledger store stream exchange relationship between two peers
type  streamLedger struct {
	Patner peer.ID

	lastExchange time.Time

	exchangeCount uint64

	wantlist *streamWantlist

	haveList *streamHavelist

	lk sync.Mutex
}

//newStreamLedger
func newStreamLedger(p peer.ID) *streamLedger{
	return &streamLedger{
		Patner: p,
		wantlist: nil,
		haveList: nil,
	}
}

func (l *streamLedger) Wants(streamid string, priority int) {
	l.wantlist.Add(streamid,priority)
}

func (l *streamLedger) CancelWant(streamid string) {
	l.wantlist.Remove(streamid)
}

func (l *streamLedger) Haves(streamid string, priority int) {
	l.wantlist.Add(streamid,priority)
}

func (l *streamLedger) CancelHave(streamid string) {
	l.wantlist.Remove(streamid)
}