package stream

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Stream struct {
	ID          string  // hash value of the stream
	Nsubstreams int     // number of substreams
	//blocklist   *blocklist	//Used to store blocks
	//				Deprecated, use datastore directly
}

type StreamBlock struct {
	StreamID string
	BlockID  cid.Cid
}


/*
 * Used to store streamblocks within a stream.
 */
type blocklist struct {

}

type StreamConfig struct {
	StreamID string
	Nsubstreams int
}

type SubStreamConfig struct {
	ID         string
	StreamMap  uint64 // 0010 means need only the second sub-stream
	StartIndex int    // only download blocks after StartIndex
}

type StreamRequest struct {
	StreamID string
	Substream int
	StartIndex int
	Requestor peer.ID
}