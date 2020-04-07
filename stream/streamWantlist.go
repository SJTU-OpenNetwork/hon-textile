package stream

type streamWantlist struct {
	set map[string]wEntry
}
// Entry is an entry in a want list, consisting of a cid and its priority
type wEntry struct {
	StreamId      string
	Priority int
}
// Add adds an entry in a wantlist from streamid & Priority, if not already present.
func (w *streamWantlist) Add(streamid string, priority int) bool {
	if _, ok := w.set[streamid]; ok {
		return false
	}

	w.set[streamid] = wEntry{
		StreamId:      streamid,
		Priority: priority,
	}

	return true
}

// Remove removes the given streamid from the wantlist.
func (w *streamWantlist) Remove(streamid string) bool {
	_, ok := w.set[streamid]
	if !ok {
		return false
	}

	delete(w.set, streamid)
	return true
}

// Contains returns the entry, if present, for the given CID, plus whether it
// was present.
func (w *streamWantlist) Contains(streamid string) (wEntry, bool) {
	e, ok := w.set[streamid]
	return e, ok
}