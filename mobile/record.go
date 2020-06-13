package mobile

import "github.com/SJTU-OpenNetwork/hon-textile/pb"
import "github.com/SJTU-OpenNetwork/hon-textile/recorder"

func (m *Mobile) GetLog(handler HlogHandler) {
	recorder.Hlog.OutputFunc(handler.HandleLog, handler.LogEnd)
}


func (m *Mobile) StartRecord(key string) error {
	return m.node.StartRecord(key)
}

func (m *Mobile) StopRecord(key string) error {
	return m.node.StopRecord(key)
}

func (m *Mobile) GetRecord(key string) (string, error) {
	return m.node.GetRecord(key)
}

func (m *Mobile) StartRecordReport(key string) error {
	return m.node.StartRecordReport(key)
}

func (m *Mobile) StopRecordReport(key string) error {
	return m.node.StopRecordReport(key)
}

func (m *Mobile) RemoveRecordReport(key string) error {
	return m.node.RemoveRecordReport(key)
}

func (m *Mobile) SendRecordReport(key string, peerId string) error {
	return m.node.SendRecordReport(key, peerId)
}

func (m *Mobile) SendRecordReportPb(report *pb.RecordReport, peerId string) error {
	return m.node.SendRecordReportPb(report, peerId)
}
