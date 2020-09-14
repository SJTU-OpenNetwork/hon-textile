package core

import (
	"context"
	"github.com/SJTU-OpenNetwork/hon-textile/util"
	ipfslite "github.com/hsanjuan/ipfs-lite"
	"github.com/textileio/go-threads/core/app"
	"github.com/textileio/go-threads/logstore/lstoreds"
	"github.com/textileio/go-threads/net"
	ipfscore "github.com/ipfs/go-ipfs/core"
	"os"
	"path"
)
// Use go-threads (https://github.com/textileio/go-threads) to cover the functionality of thread.

type ThreadService2 struct {
	net app.Net
}

// Note:
//	- repoPath must be an existing directory.
func NewThreadService2(ctx context.Context, node *ipfscore.IpfsNode, repoPath string) (*ThreadService2, error) {
	// Create logStore
	logPath := path.Join(repoPath, "threadsLog")
	if !util.DirectoryExist(logPath) {
		if err := os.Mkdir(logPath, os.ModePerm); err != nil {
			log.Error("Error when create log path for go-threads: ", err)
			return nil, err
		}
	}
	logstore, err := ipfslite.BadgerDatastore(logPath)
	if err != nil {
		log.Error("Error when create ipfslite badger datastore: ", err)
		return nil, err
	}
	tstore, err := lstoreds.NewLogstore(ctx, logstore, lstoreds.DefaultOpts())
	if err != nil {
		log.Error("Error when create logstore from ipfslite badger datastore: ", err)
		return nil, err
	}
	tmpNet, err := net.NewNetwork(
		ctx,
		node.PeerHost,
		node.Blockstore,
		node.DAG,
		tstore,
		net.Config{
			Debug: true,
			//PubSub: true,
		})
	if err != nil {
		log.Error("Error when create net.Network: ", err)
		return nil, err
	}
	return &ThreadService2{net: tmpNet}, nil
}
