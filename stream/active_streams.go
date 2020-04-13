package stream

import (
	"bytes"
	"context"
	"fmt"
	"github.com/SJTU-OpenNetwork/go-ipfs/core"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	"github.com/ipfs/go-cid"
	"sync"
)

type ErrStreamAlreadyActive struct{
	meta *pb.StreamMeta
}
type ErrStreamNotActive struct{
	streamId string
}

func (e *ErrStreamAlreadyActive) Error() string {
	return fmt.Sprintf("Stream %s is already active.", e.meta.Id)
}

func (e *ErrStreamNotActive) Error() string {
	return fmt.Sprintf("Stream %s is not active.", e.streamId)
}


/**
 * activeStreams serves as a mediumm between datastore and working environment.
 * The main routine of stream_service is moved to here.
 * activeStreams do following works
 *	- Record which stream is active now.
 *  - Record stat of each active stream. Propose stat interface.
 *  - Running routine for each stream. The routine can be stop by context.
 * TODO:
 *		Support resuming active stream from datastore.
 */
type activeStreamStore struct{
	//addStream(meta *pb.StreamMeta) error
	ctx context.Context
	lock sync.Mutex
	datastore repo.Datastore
	node func() *core.IpfsNode
	notify func(streamId string) error
	streamList map[string] *activeStream
}

func newActiveStreamStore(ctx context.Context, datastore repo.Datastore, node func() *core.IpfsNode, notify func(streamId string) error) *activeStreamStore{
	return &activeStreamStore {
		ctx: ctx,
		datastore: datastore,
		node: node,
		streamList: make(map[string] *activeStream),
		notify :notify,
	}
}

func (store *activeStreamStore) addStream(meta *pb.StreamMeta) error {
	log.Debugf("New stream %s add to activeStreamStore.", meta.Id)
	store.lock.Lock()
	defer store.lock.Unlock()
	_, ok := store.streamList[meta.Id]
	if ok {
		return &ErrStreamAlreadyActive{meta:meta}
	}
	newCtx, cancelFunc := context.WithCancel(store.ctx)
	newActiveStream := &activeStream{
		meta: meta,
		datastore: store.datastore,
		node: store.node,
		fileChan: make(chan *pb.StreamFile, 10),
		currentIndex: 0,
		ctx: newCtx,
		cancel: cancelFunc,
		notify: store.notify,
	}
	store.streamList[meta.Id] = newActiveStream
	go newActiveStream.start()
	return nil
}

func (store *activeStreamStore) stopStream(streamId string) error {
	log.Debugf("Try to stop active stream %s.", streamId)
	store.lock.Lock()
	defer store.lock.Unlock()
	s, ok := store.streamList[streamId]
	if !ok {
		return &ErrStreamNotActive{streamId:streamId}
	}
	s.fileChan <- nil //use nil to represent the end mark
	s.cancel()
	delete(store.streamList, streamId)
	return nil
}

func (store *activeStreamStore) streamAddFile(streamId string, file *pb.StreamFile) error {
	log.Debugf("Add file to active stream %s.", streamId)
	store.lock.Lock()
	defer store.lock.Unlock()
	s, ok := store.streamList[streamId]
	if !ok {
		return &ErrStreamNotActive{streamId:streamId}
	}
	s.fileChan <- file
	return nil
}

//func (store *activeStreamStore)

type activeStream struct {
	meta *pb.StreamMeta
	datastore repo.Datastore
	node func() *core.IpfsNode
	fileChan  chan *pb.StreamFile
	currentIndex uint64
	cancel context.CancelFunc
	notify func(streamId string) error
	ctx context.Context
}

func (as *activeStream) start() {
Loop:
	for{
		select {
		case f:= <- as.fileChan:
            err := as.handleNewFile(f); if err != nil {log.Error(err)}
		case <-as.ctx.Done():
			err := as.ctx.Err()
			if err != nil{
				log.Error(err)
                break Loop
			}

            // TODO: Clear file channel
            

			break Loop
		}
	}
}

func (as *activeStream) handleNewFile(f *pb.StreamFile) error {
    if f == nil {
        //TODO: handle the end mark
    } else {
	    r := bytes.NewReader(f.Data)
	    fileid, err := ipfs.AddData(as.node(), r, true, false)
	    if err != nil {
		    log.Error(err)
	    }
	    err = as.traverseNode(fileid, true, f.Description); if err != nil {return err}
	    err = as.notify(as.meta.Id); if err != nil {return err}
    }
    return nil
}
func (as *activeStream) traverseNode(cid *cid.Cid, isRoot bool, payload []byte) error {
	links, err := ipfs.LinksAtPath(as.node(), cid.String())
	if err != nil{
		return err
	}
	if len(links) == 0 {
		err = as.saveBlock(cid, isRoot, payload)
		if err != nil {
			return err
		}
	} else {
		for _,l := range links {
			err := as.traverseNode(&l.Cid, false, nil)
			if err != nil {
				return err
			}
		}
		err = as.saveBlock(cid, isRoot, payload)
		if err != nil {
			return err
		}
	}
	return nil
}

func (as *activeStream) saveBlock(cid *cid.Cid, isRoot bool, payload []byte) error {
	stat, err := ipfs.StatObjectAtPath(as.node(), cid.String())
	if err != nil {
		log.Error(err)
		return err
	}
	index := as.currentIndex
	//index := h.streamBlockIndex[sid]
	//fmt.Printf("Saving block, cid: %s, index: %d, size: %d, isroot: %d", cid.String(), cur, stat.CumulativeSize, isRoot)
	err = as.datastore.StreamBlocks().Add(&pb.StreamBlock{
		Id: cid.String(),
		Streamid: as.meta.Id,
		Index: index,
		Size: int32(stat.CumulativeSize),
		IsRoot: isRoot,
		Description: string(payload),
	})
	if err != nil {
		log.Error(err)
		return err
	}
	as.currentIndex += 1
	return nil
}


