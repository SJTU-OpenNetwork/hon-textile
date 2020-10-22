package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"net"
	"os"
	"path"
	"time"

	"github.com/SJTU-OpenNetwork/hon-textile/util"
	ipfslite "github.com/hsanjuan/ipfs-lite"
	ipfscore "github.com/ipfs/go-ipfs/core"
	"github.com/phayes/freeport"
	thread2Api "github.com/textileio/go-threads/api"
	"github.com/textileio/go-threads/api/client"
	thread2Client "github.com/textileio/go-threads/api/client"
	newthreadspb "github.com/textileio/go-threads/api/pb"
	"github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/logstore/lstoreds"
	thread2Net "github.com/textileio/go-threads/net"
	threadutil "github.com/textileio/go-threads/util"
	"google.golang.org/grpc"
)

//"github.com/textileio/go-threads/cbor"

// Use go-threads (https://github.com/textileio/go-threads) to cover the functionality of thread.

const msgTimeout = time.Second * 10
const addTimeout = time.Second * 10

type ErrThreadNoAuth struct {
	threadId string
}

func (e *ErrThreadNoAuth) Error() string {
	return fmt.Sprintf("have no privilege to access thread %s", e.threadId)
}

// NewThread2Client create the thread2 client from the running ipfs node.
// It does following things:
//	- Create logstrore for thread2.
//	- Create thread2.net from the current libp2p host.
//	- Create thread2.client from the thread2.net.
// Note:
//	- repoPath must be an existing directory.
//	- Make sure the ipfs node is already online.
func NewThread2Client(ctx context.Context, node *ipfscore.IpfsNode, repoPath string) (*thread2Client.Client, error) {
	// Create repo for thread2
	threadRepoPath := path.Join(repoPath, "threadsClient")
	if !util.DirectoryExist(threadRepoPath) {
		if err := os.Mkdir(threadRepoPath, os.ModePerm); err != nil {
			log.Error("Error when create repo path for go-threads: ", err)
			return nil, err
		}
	}

	// Create logStore
	logPath := path.Join(repoPath, "threadsLog")
	if !util.DirectoryExist(logPath) {
		if err := os.Mkdir(logPath, os.ModePerm); err != nil {
			log.Error("Error when create log path for go-threads: ", err)
			return nil, err
		}
	}

	// Create threads.net
	tmpstore, err := ipfslite.BadgerDatastore(logPath)
	if err != nil {
		log.Error("Error when create ipfslite badger datastore: ", err)
		return nil, err
	}
	tstore, err := lstoreds.NewLogstore(ctx, tmpstore, lstoreds.DefaultOpts())
	if err != nil {
		log.Error("Error when create tmpstore from ipfslite badger datastore: ", err)
		return nil, err
	}
	tmpNet, err := thread2Net.NewNetwork(
		ctx,
		node.PeerHost,
		node.Blockstore,
		node.DAG,
		tstore,
		thread2Net.Config{
			Debug:  true,
		})
	if err != nil {
		log.Error("Error when create net.Network: ", err)
		return nil, err
	}

	threadService, err := thread2Api.NewService(tmpNet, thread2Api.Config{
		RepoPath: threadRepoPath,
		Debug:    true,
	})
	if err != nil {
		log.Error("Error when create thread2 service: ", err)
		return nil, err
	}

	// Open server for threadService
	port, err := freeport.GetFreePort()
	if err != nil {
		return nil, err
	}
	//our port default is 4001,so we dont need freeport.GetFreePort(), but it seems that thread port is different with ipfs.
	addr := threadutil.MustParseAddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port))
	target, err := threadutil.TCPAddrFromMultiAddr(addr)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer()
	listener, err := net.Listen("tcp", target)
	if err != nil {
		return nil, err
	}

	// Connect server with service.
	newthreadspb.RegisterAPIServer(server, threadService)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve error: %v", err)
		}
	}()

	// Open client
	threadClient, err := client.NewClient(target, grpc.WithInsecure())
	if err != nil {
		log.Error("Error when create client: ", err)
		return nil, err
	}
	return threadClient, nil
}

type Thread2UpdateMessage struct {
	ThreadID string
	Event    client.ListenEvent
}

// Listen all thread2s
func (t *Textile) ListenThread2s() {
	dbs, err := t.threadclient.ListDBs(t.ctx)
	if err != nil {
		log.Errorf("error when list DBs", err)
	}
	for dbID, _ := range dbs {
		err := t.ListenOneThread2(dbID.String())
		if err != nil {
			log.Errorf("error when listen one thread2", err)
		}
	}
}
func (t *Textile) ListenOneThread2(dbID string) error {
	threadId, err := thread.Decode(dbID)
	if err != nil {
		return err
	}
	opt := client.ListenOption{
		Collection: "",
		InstanceID: "",
	}
	Ch, err := t.threadclient.Listen(t.ctx, threadId, []client.ListenOption{opt})
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case val, ok := <-Ch:
				if !ok {
					return
				}
				t.thread2Updates.Send(&pb.Thread2MessageUpdate{
					ThreadId: dbID,
					Collection: val.Action.Collection,
					InstanceId: val.Action.InstanceID,
					Instance: val.Action.Instance,
				})
			}
		}
	}()
	return nil
}
