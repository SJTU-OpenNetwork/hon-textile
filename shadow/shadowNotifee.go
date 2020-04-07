package shadow

import (
"github.com/libp2p/go-libp2p-core/network"
ma "github.com/multiformats/go-multiaddr"
)

type ShadowNotifee ShadowService

func (sn *ShadowNotifee) manager() *ShadowService {
	return (*ShadowService)(sn)
}

func (*ShadowNotifee) Listen (net network.Network, addr ma.Multiaddr) {
	//fmt.Printf("Notifee: Listen address %s\n", addr.String())
}

func (*ShadowNotifee) ListenClose (net network.Network, addr ma.Multiaddr) {
	//fmt.Printf("Notifee: Close listen address %s\n", addr.String())
}

func (sn *ShadowNotifee) Connected (net network.Network, conn network.Conn) {
	//fmt.Printf("Notifee: Connect %s\n", conn.RemotePeer().Pretty())
	//conn.RemoteMultiaddr()
	sn.manager().PeerConnected(conn.RemotePeer(), conn.RemoteMultiaddr())
}

func (sn *ShadowNotifee) Disconnected(net network.Network, conn network.Conn) {
	//fmt.Printf("Notifee: Disconnect %s\n", conn.RemotePeer().Pretty())
	//sn.manager().PeerDisconnected(conn.RemotePeer())
}

func (sn *ShadowNotifee) OpenedStream(net network.Network, str network.Stream) {
	//fmt.Printf("Notifee: Open stream of protocol %s\n", str.Protocol())
}

func (sn *ShadowNotifee) ClosedStream(net network.Network, str network.Stream) {
	//fmt.Printf("Notifee: Close stream of protocal %s\n", str.Protocol())
}
