package core

import "github.com/SJTU-OpenNetwork/hon-textile/pb"

func (t *Textile) StartRecord(key string) error {
	return t.record.StartRecord(key)
}

func (t *Textile) StopRecord(key string) error {
	return t.record.StopRecord(key)
}

func (t *Textile) GetRecord(key string) (string, error) {
	byteJson, err := t.record.GetRecord(key)
	if err != nil {
		return "", err
	}
	return string(byteJson), nil
}

func (t *Textile) StartRecordReport(key string) error {
	return t.record.StartReport(key)
}

func (t *Textile) StopRecordReport(key string) error {
	return t.record.StopReport(key)
}

func (t *Textile) RemoveRecordReport(key string) error {
	return t.record.RemoveReport(key)
}

func (t *Textile) SendRecordReport(key string, peerId string) error {
	return t.record.SendReport(key, peerId)
}

func (t *Textile) SendRecordReportPb(report *pb.RecordReport, peerId string) error {
	return t.record.SendReportPb(report, peerId)
}

func (t *Textile) WriteTreeCSV(streamId string, outPath string) error {
	return t.record.WriteTreeCSV(streamId, outPath)
}
