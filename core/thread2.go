package core

import (
	"context"
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/util"
	ipfslite "github.com/hsanjuan/ipfs-lite"
	ipfscore "github.com/ipfs/go-ipfs/core"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/core/app"
	"time"

	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/textileio/go-threads/core/logstore"
	"github.com/textileio/go-threads/logstore/lstoreds"
	"github.com/textileio/go-threads/net"
	netcore "github.com/textileio/go-threads/core/net"
	"os"
	"path"
)
// Use go-threads (https://github.com/textileio/go-threads) to cover the functionality of thread.

const msgTimeout = time.Second * 10
const addTimeout = time.Second * 10

type ThreadService2 struct {
	net   app.Net
	store logstore.Logstore
}

// Note:
//	- repoPath must be an existing directory.
func NewThreadService2(ctx context.Context, node *ipfscore.IpfsNode, repoPath string) (*ThreadService2, error) {
	// Create logStore
	fmt.Println("NewThreadService2")
	defer fmt.Println("Done NewThreadService2")
	logPath := path.Join(repoPath, "threadsLog")
	if !util.DirectoryExist(logPath) {
		if err := os.Mkdir(logPath, os.ModePerm); err != nil {
			log.Error("Error when create log path for go-threads: ", err)
			return nil, err
		}
	}
	fmt.Println("46")
	tmpstore, err := ipfslite.BadgerDatastore(logPath)
	if err != nil {
		log.Error("Error when create ipfslite badger datastore: ", err)
		return nil, err
	}
	fmt.Println("52")
	tstore, err := lstoreds.NewLogstore(ctx, tmpstore, lstoreds.DefaultOpts())
	if err != nil {
		log.Error("Error when create tmpstore from ipfslite badger datastore: ", err)
		return nil, err
	}
	fmt.Println("58")
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
	fmt.Println("73")
	//cbornode.RegisterCborType(make([]byte,0))
	fmt.Println("End of NewThread2")
	return &ThreadService2{net: tmpNet, store: tstore}, nil
}

func (t *Textile) Thread2List() (thread.IDSlice, error){
	fmt.Println("Thread2List")
	if t.thread2 == nil {
		fmt.Println("thread2 is nil!")
		return nil, nil
	}
	if t.thread2.store == nil {
		fmt.Println("thread2.store is nil!!")
		return nil, nil
	}
	return t.thread2.store.Threads()
}

// Thread2CreateRaw create a new thread with random id.
// "raw" means the thread has no access control.
// TODO:
//		Implement another method Thread2CreateAccess with access control.
//		Implement a method that can create thread and add meta as first node on thread.
//		Meta may contains name, access key, create time.
func (t *Textile) Thread2CreateRaw() (thread.Info, error) {
	return t.thread2.net.CreateThread(t.ctx, thread.NewIDV1(thread.Raw, 32))
}

// Thread2AddThread add an existing thread.
// Note that this method would not fetch the history of thread.
// You may need to call net.PullThread later.
func (t *Textile) Thread2AddThread(multiaddr ma.Multiaddr) (thread.Info, error) {
	actx, _ := context.WithTimeout(t.ctx, addTimeout)
	return t.thread2.net.AddThread(actx, multiaddr)
}

// Thread2AddBytes add bytes to the thread corresponding with id.
func (t *Textile) Thread2AddBytes(id thread.ID, data []byte) error {
	body, err := cbornode.WrapObject(data, mh.SHA2_256, -1)
	if err != nil {
		log.Error("Error when wrap node object: ", err)
		return err
	}
	mctx, cancel := context.WithTimeout(t.ctx, msgTimeout)
	defer cancel()
	if _, err = t.thread2.net.CreateRecord(mctx, id, body); err != nil {
		return err
	}
	return nil
}

// Thread2Subscribe return a channel to listen the update of threads.
func (t *Textile) Thread2Subscribe() (<-chan netcore.ThreadRecord, error){
	return t.thread2.net.Subscribe(t.ctx)
}