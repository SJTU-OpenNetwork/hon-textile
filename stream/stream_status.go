package stream

import (
	honlog "github.com/SJTU-OpenNetwork/hon-textile/hon-log"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"sync"
	"time"
)

type StreamInfos struct {
	infos map[string] *StreamInfo
	lock sync.Mutex
}

func NewStreamInfos() *StreamInfos {
	return &StreamInfos{
		infos: make(map[string] *StreamInfo),
	}
}

func (s *StreamInfos) clearObsoleteInfos() {
	var obsoleteStreams []string
	for sid, sinfo := range s.infos {
		if time.Since(sinfo.lastAccessTime) >= InfoObsoleteTime {
			obsoleteStreams = append(obsoleteStreams, sid)
		}
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, sid := range obsoleteStreams {
		delete (s.infos, sid)
	}

}

func (s *StreamInfos) getParent(sid string) string {
	s.lock.Lock()
	defer s.lock.Unlock()

	sinfo, ok := s.infos[sid]
	if ok {
		return sinfo.treeParent
	}
	return ""
}

func (s *StreamInfos) setParent(sid string, parent string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	sinfo, ok := s.infos[sid]
	if ok{
		sinfo.treeParent = parent
	} else {

	}

}

func (s *StreamInfos) setDuration(sid string, duration int64)  {
	s.lock.Lock()
	defer s.lock.Unlock()
	info, ok := s.infos[sid]
	if ok {
		info.streamDuration = duration
	} else {
		log.Error("No stream info when set duration ", sid)
		honlog.Hlog.Add("No stream info when set duration " + sid)
	}
}

type StreamInfo struct {
	status  pb.StreamStatus
	timer *time.Timer
	treeParent string
	streamDuration int64
	lastAccessTime time.Time
}

func (s *StreamInfos) get(id string) (*StreamInfo, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	info, ok := s.infos[id]
	return info, ok
}

func (s *StreamInfos) getOrCreate(id string) *StreamInfo {
	s.lock.Lock()
	defer s.lock.Unlock()
	info, ok := s.infos[id]
	if !ok {
		info = &StreamInfo{
			status:         pb.StreamStatus_UNKNOWN,
			timer:          nil,
			treeParent:     "",
			streamDuration: 0,
			lastAccessTime: time.Now(),
		}
		s.infos[id] = info
	}
	return info
}
