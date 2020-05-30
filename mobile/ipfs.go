package mobile

import (
	"bytes"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	"github.com/golang/protobuf/ptypes"

	ipld "github.com/ipfs/go-ipld-format"
	"github.com/golang/protobuf/proto"
	"github.com/SJTU-OpenNetwork/hon-textile/core"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
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

func (m *Mobile) DataAtFeedSimpleFile(feed []byte, cb DataCallback) {
	feedpb := &pb.FeedSimpleFile{}
	err := proto.Unmarshal(feed, feedpb)
	if err != nil {
		cb.Call(nil, "", err)
	}
	m.node.WaitAdd(1, "Mobile.DataAtFeedSimpleFile")
	go func() {
		defer m.node.WaitDone("Mobile.DataAtFeedSimpleFile")
		record := &pb.Notification{
			Block:                feedpb.Block,
			Date:                 ptypes.TimestampNow(),
			//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
			Subject:              recorder.Event_CallIPFSGet,
			Target:               feedpb.PeerId,
			Read:                 false,						// Do not send to notification channel directly
		}
		recorder.RecordCh <- record
		data, media, err := m.dataAtPath(feedpb.SimpleFile.Path)
		if err == nil {
			record2 := &pb.Notification{
				Block: feedpb.Block,
				Date:  ptypes.TimestampNow(),
				//Actor:                t.node().Identity.Pretty(),	// Whether this is id of this peer ?
				Subject: recorder.Event_DoneIPFSGet,
				Target:  feedpb.PeerId,
				Read:    false, // Do not send to notification channel directly
			}
			recorder.RecordCh <- record2
		}
		cb.Call(data, media, err)
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
		defer m.node.WaitDone("Mobile.DataAtFeedStreamFile")
        log.Debug(cid)
		data, media, err := m.dataAtPath(string(cid))
		if err == nil {
			record2 := &pb.Notification{
				Block: feedpb.Streammeta.Id,
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
