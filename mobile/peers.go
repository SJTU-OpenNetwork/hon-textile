package mobile

import (
    "github.com/golang/protobuf/proto"
	"github.com/SJTU-OpenNetwork/hon-textile/core"
)

func (m *Mobile) PeerUser(peer string) ([]byte, error) {
	if !m.node.Started() {
		return nil, core.ErrStopped
	}

    user := m.node.PeerUser(peer)
    return proto.Marshal(user)
}


