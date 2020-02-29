package stream

import (
	"fmt"
	network "github.com/libp2p/go-libp2p-core/network"
	ma "github.com/multiformats/go-multiaddr"
)

type StreamNotifee struct {

}

func (*StreamNotifee) Listen (net network.Network, addr ma.Multiaddr) {
	fmt.Printf("Notifee: Listen address %s\n", addr.String())
}

func (*StreamNotifee) ListenClose (net network.Network, addr ma.Multiaddr) {
	fmt.Printf("Notifee: Close listen address %s\n", addr.String())
}

func (*StreamNotifee) Connected (net network.Network, conn network.Conn) {
	fmt.Printf("Notifee: Connect %s\n", conn.RemotePeer().Pretty())
}

func (*StreamNotifee) Disconnected(net network.Network, conn network.Conn) {
	fmt.Printf("Notifee: Disconnect %s\n", conn.RemotePeer().Pretty())
}

func (*StreamNotifee) OpenedStream(net network.Network, str network.Stream) {
	fmt.Printf("Notifee: Open stream of protocol %s\n", str.Protocol())
}

func (*StreamNotifee) ClosedStream(net network.Network, str network.Stream) {
	fmt.Printf("Notifee: Close stream of protocal %s\n", str.Protocol())
}
