package recorder

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
)

const timeFormat = "2006-01-02 15:04:05.000"

type Record struct {
	Key string	 `json:"key"`
	createTime   time.Time
	//endTime    time.Time
	EventList    []*event `json:"events"`
}

type event struct {
	//startTime	time.Time	// get from report
	//endTime		time.Time	// get from report
	//reportTime	time.Time	// set when receive report
	DownloadTime int64 `json:"downloadTime"`
	ReportTime	 int64 `json:"reportTime"`
}

// recordStore store all the records indexed by unique key
type recordStore struct {
	store map[string]*Record
	lock sync.Mutex
}

func newRecordStore() *recordStore {
	return &recordStore{
		store: make(map[string]*Record),
	}
}

func (rs *recordStore) startRecorder(key string) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	log.Debugf("Start record %s", key)
	_, ok := rs.store[key]
	if ok {
		log.Warningf("Record %s already start. Nothing happends.", key)
		return errors.New(fmt.Sprintf("Record %s already start.", key))
	}
	rs.store[key] = &Record{createTime:time.Now(), Key:key}
	return nil
}

func (rs *recordStore) stopRecord(key string) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	log.Debugf("Stop record %s", key)
	_, ok := rs.store[key]
	if !ok {
		return errors.New(fmt.Sprintf("Record %s does not exists", key))
	}
	delete(rs.store, key)
	return nil
}

func (rs *recordStore) addReport(report *pb.RecordReport) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	receiveTime := time.Now()
	startTime, err := ptypes.Timestamp(report.Start)
	if err != nil {
		return err
	}
	endTime, err := ptypes.Timestamp(report.End)
	if err != nil {
		return err
	}

	key := report.Key
	record, ok := rs.store[key]
	if !ok {
		return errors.New(fmt.Sprintf("No such record %s", key))
	}


	newEvent := &event{
		DownloadTime: (endTime.UnixNano() - startTime.UnixNano())/1e6,
		ReportTime: (receiveTime.UnixNano() - record.createTime.UnixNano())/1e6,
	}
	record.EventList = append(record.EventList, newEvent)
	return nil
}

func (rs *recordStore) toJson(key string) ([]byte, error) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	record, ok := rs.store[key]
	if ! ok {
		return nil, errors.New(fmt.Sprintf("No such record %s", key))
	}
	return json.Marshal(record)
}

// report store temporary keep the sending report
type reportStore struct {
	store map[string]*pb.RecordReport
	lock sync.Mutex
}

func newReportStore() *reportStore {
	return &reportStore{
		store: make(map[string]*pb.RecordReport),
	}
}

func (rps *reportStore) add(key string) error {
	rps.lock.Lock()
	defer rps.lock.Unlock()
	_, ok := rps.store[key]
	if ok {
		return errors.New(fmt.Sprintf("Report %s already exists"))
	}
	rps.store[key] = &pb.RecordReport{
		Key:key,
		Start: ptypes.TimestampNow(),
	}
	return nil
}

func (rps *reportStore) stop(key string) error {
	rps.lock.Lock()
	defer rps.lock.Unlock()
	report, ok := rps.store[key]
	if !ok {
		return errors.New(fmt.Sprintf("Report %s already exists"))
	}
	report.End = ptypes.TimestampNow()
	return nil
}


func (rps *reportStore) get(key string) (*pb.RecordReport, error) {
	rps.lock.Lock()
	defer rps.lock.Unlock()
	report, ok := rps.store[key]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Report %s does not exists", key))
	}
	return report, nil
}

func (rps *reportStore) remove(key string) error {
	rps.lock.Lock()
	defer rps.lock.Unlock()
	_, ok := rps.store[key]
	if !ok {
		return errors.New(fmt.Sprintf("Report %s does not exists", key))
	}
	delete(rps.store, key)
	return nil
}