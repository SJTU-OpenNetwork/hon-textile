package core

import (
	"bytes"
	"encoding/json"
	honlog "github.com/SJTU-OpenNetwork/hon-textile/hon-log"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
	"strconv"

	//"github.com/golang/protobuf/proto"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
)

// Moved from mobile/ipfs.go
// So that desktop daemon can send notification back too.
func (t *Textile) DataAtStreamFile(feedpb *pb.FeedStreamMeta, cid []byte) ([]byte, string, error) {
	recorder.Hlog.Add("Call Data At "+feedpb.Streammeta.Id)
	if !t.started {
		return nil, "", ErrStopped
	}

	data, err := t.DataAtPath(string(cid))
	if err != nil {
		honlog.Hlog.Add("Error when call DataAtPath: "+err.Error())
		if err == ipld.ErrNotFound {
			return nil, "", nil
		}
		return nil, "", err
	}

    //t.stream.FlushStreamDatabase(feedpb, string(cid))
	media, err := t.GetMedia(bytes.NewReader(data))
	if err != nil {
		honlog.Hlog.Add("Error when call GetMedia: " + err.Error())
		return nil, "", err
	}

	sid := feedpb.Streammeta.Id
	duration := t.stream.GetDuration(sid)

	block_map := map[string] string {
		"ID": sid,
		"Parent": t.StreamGetParent(sid),
		"Duration": strconv.FormatInt(duration,10),
	}
	block_json, err := json.Marshal(block_map)
	if err != nil {
		honlog.Hlog.Add("Error when marshal json" + err.Error())
		log.Error(err)
	}

	record2 := &pb.Notification{
		Block: sid,
		Date:  ptypes.TimestampNow(),
		//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
		Subject: recorder.Event_DoneIPFSGet,
		Body: string(block_json),
		Target:  feedpb.PeerId,
		Read:    false, // Do not send to notification channel directly
	}
	recorder.RecordCh <- record2
	recorder.Hlog.Add("Done Data At "+feedpb.Streammeta.Id)
	return data, media, nil
}
