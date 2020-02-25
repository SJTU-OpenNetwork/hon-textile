package stream

//	Deprecated, use datastore and pb directly
//type Stream struct {
//	ID          string  // hash value of the stream
//	Nsubstreams int     // number of substreams
//	blocklist   *blocklist	//Used to store blocks
//
//}

// Deprecated, use datastore and pb directly
//type StreamBlock struct {
//	StreamID string
//	BlockID  cid.Cid
//}

// Deprecated
//type blocklist struct {
//
//}

//type StreamConfig struct {
//	StreamID string
//	Nsubstreams int
//}

//type SubStreamConfig struct {
//	ID         string
//	StreamMap  uint64 // 0010 means need only the second sub-stream
//	StartIndex uint64    // only download blocks after StartIndex
//}

// Deprecated, use substreamconfig instead
//type StreamRequest struct {
//	StreamID string
//	Substream int
//	StartIndex uint64
//	Requestor peer.ID
//}
