package core

import (
	"bytes"
	"encoding/json"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
	//"github.com/golang/protobuf/proto"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
)

// Moved from mobile/ipfs.go
// So that desktop daemon can send notification back too.
func (t *Textile) DataAtStreamFile(feedpb *pb.FeedStreamMeta, cid []byte) ([]byte, string, error) {
	if !t.started {
		return nil, "", ErrStopped
	}

	data, err := t.DataAtPath(string(cid))
	if err != nil {
		if err == ipld.ErrNotFound {
			return nil, "", nil
		}
		return nil, "", err
	}

	media, err := t.GetMedia(bytes.NewReader(data))
	if err != nil {
		return nil, "", err
	}

	sid := feedpb.Streammeta.Id
	block_map := map[string] string {
		"ID": sid,
		"Parent": t.StreamGetParent(sid),
	}
	block_json, err := json.Marshal(block_map)
	if err != nil {
		log.Error(err)
	}

	record2 := &pb.Notification{
		Block: string(block_json),
		Date:  ptypes.TimestampNow(),
		//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
		Subject: recorder.Event_DoneIPFSGet,
		Target:  feedpb.PeerId,
		Read:    false, // Do not send to notification channel directly
	}
	recorder.RecordCh <- record2

	return data, media, nil
}
