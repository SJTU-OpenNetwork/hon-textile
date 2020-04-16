package stream

type streamHavelist struct {
	set map[string]hEntry
}
// Entry is an entry in a have list, consisting of a stream id and its priority
type hEntry struct {
	StreamId      string
	Priority int
}
// Add adds an entry in a havelist from streamid & Priority
func (h *streamHavelist) Add(streamid string, priority int) bool {
	if _, ok := h.set[streamid]; ok {
		return false
	}

	h.set[streamid] = hEntry{
		StreamId:      streamid,
		Priority: priority,
	}
	return true
}

// Remove removes the given streamid from the havelist.
func (h *streamHavelist) Remove(streamid string) bool {
	_, ok := h.set[streamid]
	if !ok {
		return false
	}

	delete(h.set, streamid)
	return true
}

// Contains returns the entry, if present, for the given stream id
func (h *streamHavelist) Contains(streamid string) (hEntry, bool) {
	e, ok := h.set[streamid]
	return e, ok
}