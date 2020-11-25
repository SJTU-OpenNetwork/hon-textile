package mobile

import (
	"bytes"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	"github.com/SJTU-OpenNetwork/hon-textile/util"
	"github.com/golang/protobuf/ptypes"
	//"google.golang.org/protobuf/types/known/timestamppb"
	tspb "github.com/golang/protobuf/ptypes/timestamp"

	"github.com/SJTU-OpenNetwork/hon-textile/core"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/golang/protobuf/proto"
	ipld "github.com/ipfs/go-ipld-format"
)

func (m *Mobile) GetSwarmAddress(pid string) (string, error) {
	if !m.node.Started() {
		return "", core.ErrStopped
    }

    mpid, err := m.node.PeerId()
    if err != nil {
        log.Error(err)
        return "", err
    }
    if pid == mpid.Pretty() {
        log.Debug("Its me!")
        log.Debug(m.node.MySwarmAddress())
        return m.node.MySwarmAddress(), nil
    }
    return m.node.GetSwarmAddress(pid), nil
}

func (m *Mobile) ConnectedAddresses() ([]byte, error) {
	if !m.node.Started() {
		return nil, core.ErrStopped
    }
    swarmPeers, err := m.node.ConnectedAddresses()
    if err != nil {
        return nil, err
    }
	return proto.Marshal(swarmPeers)
}

// PeerId returns the ipfs peer id
func (m *Mobile) PeerId() (string, error) {
	if !m.node.Started() {
		return "", core.ErrStopped
	}

	pid, err := m.node.PeerId()
	if err != nil {
		return "", err
	}
	return pid.Pretty(), nil
}

// SwarmConnect opens a new direct connection to a peer using an IPFS multiaddr
func (m *Mobile) SwarmConnect(address string) (string, error) {
	if !m.node.Started() {
		return "", core.ErrStopped
	}

	results, err := ipfs.SwarmConnect(m.node.Ipfs(), []string{address})
	if err != nil {
		return "", err
	}

	return results[0], nil
}

// DataAtPath is the async version of dataAtPath
func (m *Mobile) DataAtPath(pth string, cb DataCallback) {
	m.node.WaitAdd(1, "Mobile.DataAtPath")
	go func() {
		defer m.node.WaitDone("Mobile.DataAtPath")
		cb.Call(m.dataAtPath(pth))
	}()
}

// dataAtPath calls core DataAtPath
func (m *Mobile) dataAtPath(pth string) ([]byte, string, error) {
	if !m.node.Started() {
		return nil, "", core.ErrStopped
	}

	data, err := m.node.DataAtPath(pth)
	if err != nil {
		if err == ipld.ErrNotFound {
			return nil, "", nil
		}
		return nil, "", err
	}

	media, err := m.node.GetMedia(bytes.NewReader(data))
	if err != nil {
		return nil, "", err
	}

	return data, media, nil
}

func (m *Mobile) pathAtFolder(pth string) (string, error){
	if !m.node.Started() {
		return "", core.ErrStopped
	}

	path, err := m.node.FolderAtPath(pth)
	if err != nil {
		if err == ipld.ErrNotFound {
			return "", nil
		}
		return "", err
	}

	return path,nil
}

func (m *Mobile) BigFileAtSimpleFile(feed []byte, cb PathCallbackWithTime) {
	feedpb := &pb.FeedSimpleFile{}
	err := proto.Unmarshal(feed, feedpb)
	if err != nil {
		cb.Call("", "", 0, err)
	}
	m.node.WaitAdd(1, "Mobile.DataAtFeedSimpleFile")
	go func() {
		defer m.node.WaitDone("Mobile.DataAtFeedSimpleFile")
		var ipfsGetTime *tspb.Timestamp
		var ipfsDoneTime *tspb.Timestamp
		ipfsGetTime = ptypes.TimestampNow()
		record := &pb.Notification{
			Block:                feedpb.Block,
			//Date:                 ptypes.TimestampNow(),
			Date:					ipfsGetTime,
			//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
			Subject:              recorder.Event_CallIPFSGet,
			Target:               feedpb.PeerId,
			Read:                 false,						// Do not send to notification channel directly
		}
		recorder.RecordCh <- record
		//data, media, err := m.dataAtPath(feedpb.SimpleFile.Path)
		tmpPath, err := m.node.FilePathAtPath(feedpb.SimpleFile.Path)
		ipfsDoneTime = ptypes.TimestampNow()
		if err == nil {
			record2 := &pb.Notification{
				Block: feedpb.Block,
				//Date:  ptypes.TimestampNow(),
				Date: 	ipfsDoneTime,
				//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
				Subject: recorder.Event_DoneIPFSGet,
				Target:  feedpb.PeerId,
				Read:    false, // Do not send to notification channel directly
			}
			recorder.RecordCh <- record2
		}

		cb.Call(tmpPath, "", util.ProtoDuration(ipfsGetTime, ipfsDoneTime), err)
	}()
}

func (m *Mobile) DataAtFeedSimpleFile(feed []byte, cb DataCallbackWithTime) {
	feedpb := &pb.FeedSimpleFile{}
	err := proto.Unmarshal(feed, feedpb)
	if err != nil {
		cb.Call(nil, "", 0, err)
	}
	m.node.WaitAdd(1, "Mobile.DataAtFeedSimpleFile")
	go func() {
		defer m.node.WaitDone("Mobile.DataAtFeedSimpleFile")
		var ipfsGetTime *tspb.Timestamp
		var ipfsDoneTime *tspb.Timestamp
		ipfsGetTime = ptypes.TimestampNow()
		record := &pb.Notification{
			Block:                feedpb.Block,
			//Date:                 ptypes.TimestampNow(),
			Date:					ipfsGetTime,
			//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
			Subject:              recorder.Event_CallIPFSGet,
			Target:               feedpb.PeerId,
			Read:                 false,						// Do not send to notification channel directly
		}
		recorder.RecordCh <- record
		data, media, err := m.dataAtPath(feedpb.SimpleFile.Path)
		ipfsDoneTime = ptypes.TimestampNow()
		if err == nil {
			record2 := &pb.Notification{
				Block: feedpb.Block,
				//Date:  ptypes.TimestampNow(),
				Date:	ipfsDoneTime,
				//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
				Subject: recorder.Event_DoneIPFSGet,
				Target:  feedpb.PeerId,
				Read:    false, // Do not send to notification channel directly
			}
			recorder.RecordCh <- record2
		}
		cb.Call(data, media, util.ProtoDuration(ipfsGetTime, ipfsDoneTime), err)
	}()
}

func (m *Mobile) DataAtFeedSimpleFolder(feed []byte, cb PathCallbackWithTime){
	feedpb := &pb.FeedSimpleFile{}
	err := proto.Unmarshal(feed, feedpb)
	if err != nil {
		cb.Call("","", 0,err)
	}
	m.node.WaitAdd(1, "Mobile.DataAtFeedSimpleFolder")
	go func() {
		defer m.node.WaitDone("Mobile.DataAtFeedSimpleFolder")
		var ipfsGetTime *tspb.Timestamp
		var ipfsDoneTime *tspb.Timestamp
		ipfsGetTime = ptypes.TimestampNow()
		record := &pb.Notification{
			Block:                feedpb.Block,
			//Date:                 ptypes.TimestampNow(),
			Date:					ipfsGetTime,
			//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
			Subject:              recorder.Event_CallIPFSGet,
			Target:               feedpb.PeerId,
			Read:                 false,						// Do not send to notification channel directly
		}
		recorder.RecordCh <- record
		path, err := m.pathAtFolder(feedpb.SimpleFile.Path)
		ipfsDoneTime = ptypes.TimestampNow()
		if err == nil {
			record2 := &pb.Notification{
				Block: feedpb.Block,
				//Date:  ptypes.TimestampNow(),
				Date:	ipfsDoneTime,
				//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
				Subject: recorder.Event_DoneIPFSGet,
				Target:  feedpb.PeerId,
				Read:    false, // Do not send to notification channel directly
			}
			recorder.RecordCh <- record2
		}
		cb.Call(path, "",util.ProtoDuration(ipfsGetTime, ipfsDoneTime), err)
	}()
}

func (m *Mobile) BigFileAtStream(feed []byte, cid []byte, cb PathCallback) {
	feedpb := &pb.FeedStreamMeta{}
	err := proto.Unmarshal(feed, feedpb)
	if err != nil {
		cb.Call("", "", err)
	}
	m.node.WaitAdd(1, "Mobile.DataAtStreamFile")
	go func() {
		defer m.node.WaitDone("Mobile.DataAtStreamFile")
		cb.Call(m.node.BigFileAtStream(feedpb, cid))
	}()
}

func (m *Mobile) DataAtStreamFile(feed []byte, cid []byte, cb DataCallback) {
	feedpb := &pb.FeedStreamMeta{}
	err := proto.Unmarshal(feed, feedpb)
	if err != nil {
		cb.Call(nil, "", err)
	}
	m.node.WaitAdd(1, "Mobile.DataAtStreamFile")
	go func() {
		defer m.node.WaitDone("Mobile.DataAtStreamFile")
		/*
        log.Debug(cid)
		data, media, err := m.dataAtPath(string(cid))
		if err == nil {
            sid := feedpb.Streammeta.Id
            block_map := map[string] string {
                "ID": sid,
                "Parent": m.node.StreamGetParent(sid),
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
		} else {
            log.Error(err)
        }
		cb.Call(data, media, err)
		*/
		cb.Call(m.node.DataAtStreamFile(feedpb, cid))
	}()
}

// IpfsAddData is the async version of ipfsAddData
func (m *Mobile) IpfsAddData(data []byte, pin bool, hashOnly bool, cb IpfsAddDataCallback) {
	m.node.WaitAdd(1, "Mobile.IpfsAddData")
	go func() {
		defer m.node.WaitDone("Mobile.IpfsAddData")
		cb.Call(m.ipfsAddData(data, pin, hashOnly))
	}()
}

// ipfsAddData calls core AddData
func (m *Mobile) ipfsAddData(data []byte, pin bool, hashOnly bool) (string, error) {
	if !m.node.Started() {
		return "", core.ErrStopped
	}

	path, err := m.node.AddData(data, pin, hashOnly)
	if err != nil {
		return "", err
	}

	return path, nil
}

func (m *Mobile) ObjectAtPath(pth string) ([]byte, error) {
	if !m.node.Started() {
		return nil, core.ErrStopped
	}
	return ipfs.ObjectAtPath(m.node.Ipfs(), pth)
}

func (m *Mobile) IpfsComparePath(pth1 string, pth2 string, cb IpfsCompareCallback) {
	if !m.node.Started() {
		cb.Call(0,0,0, core.ErrStopped)
	}
	m.node.WaitAdd(1, "Mobile.IpfsComparePath")
	go func() {
		defer m.node.WaitDone("Mobile.IpfsComparePath")
		cb.Call(ipfs.ComparePath(m.node.Ipfs(), pth1, pth2))
	}()
}

func (m *Mobile) IpfsListCids(pth string, cb IpfsListPathCallback) {
	if !m.node.Started() {
		cb.OnError(core.ErrStopped)
	}
	list, err := ipfs.ListSortCids(m.node.Ipfs(), pth)
	if err != nil {
		cb.OnError(err)
		return
	}

	for _, s := range list {
		cb.OnCid(s)
	}
	cb.OnComplete()
}