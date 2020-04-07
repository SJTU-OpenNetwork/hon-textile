package stream

type streamHavelist struct {
	set map[string]hEntry
}
// Entry is an entry in a want list, consisting of a cid and its priority
type hEntry struct {
	StreamId      string
	Priority int
}
