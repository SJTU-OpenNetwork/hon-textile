package config

import (
	"fmt"
	"time"

	ipfs "github.com/ipfs/go-ipfs"
	native "github.com/ipfs/go-ipfs-config"
	"github.com/libp2p/go-libp2p-core/peer"
)

// DefaultServerFilters has is a list of IPv4 and IPv6 prefixes that are private, local only, or unrouteable.
// according to https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml
// and https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml
var DefaultServerFilters = []string{
	"/ip4/10.0.0.0/ipcidr/8",
	"/ip4/100.64.0.0/ipcidr/10",
	"/ip4/169.254.0.0/ipcidr/16",
	"/ip4/172.16.0.0/ipcidr/12",
	"/ip4/192.0.0.0/ipcidr/24",
	"/ip4/192.0.0.0/ipcidr/29",
	"/ip4/192.0.0.8/ipcidr/32",
	"/ip4/192.0.0.170/ipcidr/32",
	"/ip4/192.0.0.171/ipcidr/32",
	"/ip4/192.0.2.0/ipcidr/24",
	"/ip4/192.168.0.0/ipcidr/16",
	"/ip4/198.18.0.0/ipcidr/15",
	"/ip4/198.51.100.0/ipcidr/24",
	"/ip4/203.0.113.0/ipcidr/24",
	"/ip4/240.0.0.0/ipcidr/4",
	"/ip6/100::/ipcidr/64",
	"/ip6/2001:2::/ipcidr/48",
	"/ip6/2001:db8::/ipcidr/32",
	"/ip6/fc00::/ipcidr/7",
	"/ip6/fe80::/ipcidr/10",

}

// DefaultBootstrapAddresses are the addresses of cafe nodes run by the Textile team.
var DefaultBootstrapAddresses = []string{
//	"/ip4/104.210.43.77/tcp/4001/ipfs/12D3KooWSdGmRz5JQidqrtmiPGVHkStXpbSAMnbCcW8abq6zuiDP",  // us-west
//	"/ip4/20.39.232.27/tcp/4001/ipfs/12D3KooWLnUv9MWuRM6uHirRPBM4NwRj54n4gNNnBtiFiwPiv3Up",   // eu-west
//	"/ip4/34.87.103.105/tcp/4001/ipfs/12D3KooWA5z2C3z1PNKi36Bw1MxZhBD8nv7UbB7YQP6WcSWYNwRQ",  // as-southeast
//	"/ip4/18.144.12.135/tcp/4001/ipfs/12D3KooWGBW3LfzypK3zgV4QxdPyUm3aEuwBDMKRRpCPm9FrJvar",  // us-west-1a
//	"/ip4/13.57.23.210/tcp/4001/ipfs/12D3KooWQue2dSRqnZTVvikoxorZQ5Qyyug3hV65rYnWYpYsNMRE",   // us-west-1c
//	"/ip4/13.56.163.77/tcp/4001/ipfs/12D3KooWFrrmGJcQhE5h6VUvUEXdLH7gPKdWh2q4CEM62rFGcFpr",   // us-west-beta
//	"/ip4/52.53.127.155/tcp/4001/ipfs/12D3KooWGN8VAsPHsHeJtoTbbzsGjs2LTmQZ6wFKvuPich1TYmYY",  // us-west-dev
//	"/ip4/18.221.167.133/tcp/4001/ipfs/12D3KooWERmHT6g4YkrPBTmhfDLjfi8b662vFCfvBXqzcdkPGQn1", // us-east-2a
//	"/ip4/18.224.173.65/tcp/4001/ipfs/12D3KooWLh9Gd4C3knv4XqCyCuaNddfEoSLXgekVJzRyC5vsjv5d",  // us-east-2b
//	"/ip4/35.180.16.103/tcp/4001/ipfs/12D3KooWDhSfXZCBVAK6SNQu7h6mfGCBJtjMS44PW5YA5YCjVmjB",  // eu-west-3a
//	"/ip4/35.180.35.45/tcp/4001/ipfs/12D3KooWBCZEDkZ2VxdNYKLLUACWbXMvW9SpVbbvoFR9CtH4qJv9",   // eu-west-3b
//	"/ip4/13.250.53.27/tcp/4001/ipfs/12D3KooWQ5MR9Ugz9HkVU3fYFbiWbQR4jxKJB66JoSY7nP5ShsqQ",   // ap-southeast-1a
//	"/ip4/3.1.49.130/tcp/4001/ipfs/12D3KooWDWJ473M3fXMEcajbaGtqgr6i6SvDdh5Ru9i5ZzoJ9Qy8",     // ap-southeast-1b
}

// DefaultHuaweiBootstrapAddresses are the addresses of nodes run by the SJTU opennetwork team.
var DefaultOpennetBootstrapAddresses = []string{
//	"/ip4/159.138.132.28/tcp/48487/ipfs/12D3KooWHS9NbK2N7uPtYoQ3YPtFCgPX9G138REeJ1QxMTGuvpxc",  // HW3 
//	"/ip4/159.138.130.106/tcp/36387/ipfs/12D3KooWGsaEnG3tZyWwBXqfm8wd93WogDeLarMQetPdVzwZFLc1",   // HW4
//	"/ip4/159.138.130.129/tcp/4277/ipfs/12D3KooWDZuZ8sppVD37gAZVYbNrfUNqSyT2Ra8wDLXifuEgDQvC",  // HW5
//	"/ip4/159.138.58.61/tcp/41806/ipfs/12D3KooWBZ8UJeFcK7VgyrNk5s3M2PRpfpTzb1LJqMYazC3NDX8e",  // HW6
//	"/ip4/202.120.38.100/tcp/22612/ipfs/12D3KooWMsnTufaczYVEMYEo3q3PP9XrkJvHMEfQark6sZoEVm9U",   // LAB-100

// disable HK servers
//    "/ip4/159.138.132.28/tcp/4001/ipfs/QmVsZsogPLnJE2rzejZmjmpdPKKd33UkdJuFvBniefNAMn",
//    "/ip4/159.138.130.129/tcp/4001/ipfs/QmZX8WVgJ3cQCW3bNcodXhmK34rmNkvqk8Zg9u7f3JEFgN",
//    "/ip4/159.138.58.61/tcp/4001/ipfs/QmT2pfx89HyJ5eLVn15AV2JXuUEyQHpWMZxXjn3bZ997wc",
//    "/ip4/159.138.130.106/tcp/4001/ipfs/QmXn48f5fbZJmqCaUN184UpFeK36jWMMc5knT7mjbjz7Co",
    "/ip6/2001:da8:8000:6084:1a31:bfff:fecf:e603/tcp/12589/ipfs/12D3KooWEBKQAdjyqa4iMp8Lu8NF9tMQSWZoninNNPhbGYJ1xvcH",
    "/ip4/202.120.38.131/tcp/12589/ipfs/12D3KooWEBKQAdjyqa4iMp8Lu8NF9tMQSWZoninNNPhbGYJ1xvcH",
    "/ip4/202.120.38.100/tcp/4001/ipfs/QmZt8jsim548Y5UFN24GL9nX9x3eSS8QFMsbSRNMBAqKBb",
    "/ip4/139.9.123.113/tcp/4001/ipfs/QmRHLRg5vihUgakbk7JnQFswWu7D92awdZnKiQRi1DmJhE",
    "/ip4/119.3.23.219/tcp/4001/ipfs/QmYBXdc56TrPqKWhAYJZneLpVeG4qMaV8Be6yox3fiqBYd",
    "/ip4/121.36.167.61/tcp/4001/ipfs/QmYL5AAcaGA2undBnRqWRTmndkL1YV3v7tML8DbakC8sTD",
    "/ip4/122.112.199.88/tcp/4001/ipfs/QmcwtfsFoJALLQwJWmsh5SmothbrniohPcW2PuggSVKurT",
    "/ip4/119.3.24.157/tcp/4001/ipfs/QmYCYQMhyDJV4BU9fRr5xBzFDEccnukuViUT7GJLngP7fj",
}

var OpennetCafeAddresses = []string{
    "/ip6/2001:da8:8000:6084:1a31:bfff:fecf:e603/tcp/12589/ipfs/12D3KooWEBKQAdjyqa4iMp8Lu8NF9tMQSWZoninNNPhbGYJ1xvcH",
    "/ip4/202.120.38.131/tcp/12589/ipfs/12D3KooWEBKQAdjyqa4iMp8Lu8NF9tMQSWZoninNNPhbGYJ1xvcH",
//    "/ip4/159.138.58.61/tcp/26056/ipfs/12D3KooWKAwHi4WkUY5qbg2psSpDSZ4EsLwjaiCjC8ERajwZsXHc",
}

// TextileBootstrapPeers returns the (parsed) set of Textile bootstrap peers.
func TextileBootstrapPeers() ([]peer.AddrInfo, error) {
	ps, err := native.ParseBootstrapPeers(DefaultBootstrapAddresses)
	if err != nil {
		return nil, fmt.Errorf(`failed to parse hardcoded bootstrap peers: %s
This is a problem with the Textile codebase. Please report it to the dev team.`, err)
	}
	return ps, nil
}

// OpennetBootstrapPeers returns the (parsed) set of Opennet bootstrap peers.
func OpennetBootstrapPeers() ([]peer.AddrInfo, error) {
	ps, err := native.ParseBootstrapPeers(DefaultOpennetBootstrapAddresses)
	if err != nil {
		return nil, fmt.Errorf(`failed to parse hardcoded bootstrap peers: %s
This is a problem with the Textile codebase. Please report it to the dev team.`, err)
	}
	return ps, nil
}

func OpennetCafes() ([]peer.AddrInfo, error) {
	ps, err := native.ParseBootstrapPeers(OpennetCafeAddresses)
	if err != nil {
		return nil, fmt.Errorf(`failed to parse hardcoded bootstrap peers: %s
This is a problem with the Textile codebase. Please report it to the dev team.`, err)
	}
	return ps, nil
}


// InitIpfs create the IPFS config file
func InitIpfs(identity native.Identity, mobile bool, server bool) (*native.Config, error) {
//  	ipfsPeers, err := native.DefaultBootstrapPeers()
//  	if err != nil {
//  		return nil, err
//  	}
//  	textilePeers, err := TextileBootstrapPeers()
//  	if err != nil {
//  		return nil, err
//  	}
	opennetPeers, err := OpennetBootstrapPeers()
	if err != nil {
		return nil, err
	}
//	peers := append(opennetPeers, append(textilePeers, ipfsPeers...)...)
    peers := opennetPeers

	var addrFilters []string
	if server {
		addrFilters = DefaultServerFilters
	}

	routing := "dhtclient"
	reprovider := "0"
	connMgrLowWater := 600
	connMgrHighWater := 900
	connMgrGracePeriod := time.Second * 20
	if mobile {
		connMgrLowWater = 200
		connMgrHighWater = 500
	}
	if server {
		routing = "dht"
		reprovider = "12h"
	}

	conf := &native.Config{
		API: native.API{
			HTTPHeaders: map[string][]string{
				"Server": {"go-ipfs/" + ipfs.CurrentVersionNumber},
			},
		},

		// setup the node's default addresses.
		// NOTE: two swarm listen addrs, one tcp, one utp.
		Addresses: addressesConfig(server),

		Datastore: defaultDatastoreConfig(),
		Bootstrap: native.BootstrapPeerStrings(peers),
		Identity:  identity,
		Discovery: native.Discovery{
			MDNS: native.MDNS{
				Enabled:  !server,
				Interval: 3,
			},
		},

		Routing: native.Routing{
			Type: routing,
		},

		// setup the node mount points.
		Mounts: native.Mounts{
			IPFS: "/ipfs",
			IPNS: "/ipns",
		},

		Ipns: native.Ipns{
			ResolveCacheSize: 128,
		},

		Gateway: native.Gateway{
			RootRedirect: "",
			Writable:     false,
			PathPrefixes: []string{},
			HTTPHeaders: map[string][]string{
				"Access-Control-Allow-Origin":  {"*"},
				"Access-Control-Allow-Methods": {"GET"},
				"Access-Control-Allow-Headers": {"X-Requested-With", "Range"},
			},
			APICommands: []string{},
		},
		Reprovider: native.Reprovider{
			Interval: reprovider,
			Strategy: "all",
		},
		Swarm: native.SwarmConfig{
			AddrFilters: addrFilters,
			ConnMgr: native.ConnMgr{
				LowWater:    connMgrLowWater,
				HighWater:   connMgrHighWater,
				GracePeriod: connMgrGracePeriod.String(),
				Type:        "basic",
			},
			DisableBandwidthMetrics: mobile,
			DisableNatPortMap:       server,
			DisableRelay:            false,
			EnableRelayHop:          server,
			EnableAutoRelay:         !server,
			EnableAutoNATService:    server,
		},
		Experimental: native.Experiments{
			FilestoreEnabled:     false,
			ShardingEnabled:      false,
			Libp2pStreamMounting: false,
		},
		Pubsub: native.PubsubConfig{
			Router: "gossipsub",
		},
	}

	return conf, nil
}

func addressesConfig(server bool) native.Addresses {
	noAnnounce := DefaultServerFilters
	//noAnnounce := make([]string, 0)
	//if server {
	//	noAnnounce = DefaultServerFilters
	//}
	return native.Addresses{
		Swarm:      []string{"/ip6/::/tcp/4001"},
		Announce:   []string{},
		NoAnnounce: noAnnounce,
		API:        []string{"/ip4/127.0.0.1/tcp/5001"},
		Gateway:    []string{"/ip4/127.0.0.1/tcp/8080"},
	}
}

// DefaultDatastoreConfig is an internal function exported to aid in testing.
func defaultDatastoreConfig() native.Datastore {
	return native.Datastore{
		StorageMax:         "10GB",
		StorageGCWatermark: 90, // 90%
		GCPeriod:           "1h",
		BloomFilterSize:    0,
		Spec: map[string]interface{}{
			"type": "mount",
			"mounts": []interface{}{
				map[string]interface{}{
					"mountpoint": "/blocks",
					"type":       "measure",
					"prefix":     "flatfs.datastore",
					"child": map[string]interface{}{
						"type":      "flatfs",
						"path":      "blocks",
						"sync":      true,
						"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
					},
				},
				map[string]interface{}{
					"mountpoint": "/",
					"type":       "measure",
					"prefix":     "leveldb.datastore",
					"child": map[string]interface{}{
						"type":        "levelds",
						"path":        "datastore",
						"compression": "none",
					},
				},
			},
		},
	}
}
