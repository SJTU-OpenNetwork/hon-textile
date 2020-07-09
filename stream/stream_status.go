package stream

import (
	"fmt"
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

func (s *StreamInfos) getDuration(sid string) int64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	info, ok := s.infos[sid]
	if ok {
		return info.streamDuration
	} else {
		log.Error("No stream info when get duration ", sid)
		honlog.Hlog.Add("No stream info when get duration " + sid)
		return 0
	}
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
			status:         pb.StreamStatus_NEW,
			timer:          nil,
			treeParent:     "",
			streamDuration: 0,
			lastAccessTime: time.Now(),
		}
		s.infos[id] = info
	}
	return info
}

type StreamInfo struct {
	status  pb.StreamStatus
	sLock sync.Mutex
	timer *time.Timer
	treeParent string
	streamDuration int64
	lastAccessTime time.Time
}

func (info *StreamInfo) onInform() {
	info.sLock.Lock()
	defer info.sLock.Unlock()
	switch info.status {
	case pb.StreamStatus_NEW:
		honlog.Hlog.Add(fmt.Sprintf("[%s] %s ==> %s", TAG_STATUS, info.status.String(), pb.StreamStatus_REQUESTING.String()))
		//time.AfterFunc(Inform)
		//info.status
		info.status = pb.StreamStatus_REQUESTING
	case pb.StreamStatus_NO_INFORM:
		if info.timer == nil {
			log.Error("No timer when receive inform")
			honlog.Hlog.Add("Error: no timer when receive inform.")
		} else {
			info.timer.Stop()
		}
		info.status = pb.StreamStatus_REQUESTING
		honlog.Hlog.Add(fmt.Sprintf("[%s] %s ==> %s", TAG_STATUS, info.status.String(), pb.StreamStatus_REQUESTING.String()))
	default:
		log.Error("Wrong status when get inform: ", info.status.String())
		honlog.Hlog.Add("Error, Wrong status when get inform: " + info.status.String())
	}
}

func (info *StreamInfo) onRequestSuccess(timeout func()) {
	info.sLock.Lock()
	defer info.sLock.Unlock()
	switch info.status {
	case pb.StreamStatus_REQUESTING:
		info.timer = time.AfterFunc(RecvTimeout, timeout)
		info.status = pb.StreamStatus_RECEIVING
		honlog.Hlog.Add(fmt.Sprintf("[%s] %s ==> %s", TAG_STATUS, info.status.String(), pb.StreamStatus_RECEIVING.String()))
    default:
		log.Error("Wrong status when handling request success: ", info.status.String())
		honlog.Hlog.Add("Error, Wrong status when handling request success: " + info.status.String())
    }
}

func (info *StreamInfo) onMeta(timeout func()) {
	info.sLock.Lock()
	defer info.sLock.Unlock()
	switch info.status {
	case pb.StreamStatus_NEW:
		info.timer = time.AfterFunc(InformTimeOut, timeout)
		info.status = pb.StreamStatus_NO_INFORM
		honlog.Hlog.Add(fmt.Sprintf("[%s] %s ==> %s", TAG_STATUS, info.status.String(), pb.StreamStatus_NO_INFORM.String()))
	case pb.StreamStatus_COMPLETE:
		honlog.Hlog.Add("Stream has already been received")
	default:
		log.Debugf("[%s] Status is %s when receive meta.", TAG_STATUS, info.status.String())
	}
}

func (info *StreamInfo) onCreateStream() {
	info.sLock.Lock()
	defer info.sLock.Unlock()
	switch info.status {
	case pb.StreamStatus_NEW:
		info.status = pb.StreamStatus_COMPLETE
		honlog.Hlog.Add(fmt.Sprintf("[%s] %s ==> %s", TAG_STATUS, info.status.String(), pb.StreamStatus_COMPLETE.String()))
	default:
		log.Error("Wrong status when create stream: ", info.status.String())
		honlog.Hlog.Add("Error, Wrong status when create stream: " + info.status.String())
	}
}

func (info *StreamInfo) refreshProviderTimer() {
	info.sLock.Lock()
	defer info.sLock.Unlock()
	switch info.status {
	case pb.StreamStatus_RECEIVING:
		if info.timer != nil {
			info.timer.Reset(RecvTimeout)
		}
	default:
		log.Error("Wrong status when refresh provider timer: ", info.status.String())
		honlog.Hlog.Add("Error, Wrong status when refresh provider timer: " + info.status.String())
	}
}

