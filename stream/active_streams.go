package stream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	"github.com/SJTU-OpenNetwork/hon-textile/repo"
	"github.com/golang/protobuf/ptypes"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs/core"
)

type ErrStreamAlreadyActive struct {
	meta *pb.StreamMeta
}
type ErrStreamNotActive struct {
	streamId string
}

func (e *ErrStreamAlreadyActive) Error() string {
	return fmt.Sprintf("Stream %s is already active.", e.meta.Id)
}

func (e *ErrStreamNotActive) Error() string {
	return fmt.Sprintf("Stream %s is not active.", e.streamId)
}

/**
 * activeStreams serves as a medium between datastore and working environment.
 * The main routine of stream_service is moved to here.
 * activeStreams do following works
 *	- Record which stream is active now.
 *  - Record stat of each active stream. Propose stat interface.
 *  - Running routine for each stream. The routine can be stop by context.
 * TODO:
 *		Support resuming active stream from datastore.
 */
type activeStreamStore struct {
	//addStream(meta *pb.StreamMeta) error
	ctx        context.Context
	lock       sync.Mutex
	datastore  repo.Datastore
	node       func() *core.IpfsNode
	notify     func(streamId string) error
	streamList map[string]*activeStream
}

func newActiveStreamStore(ctx context.Context, datastore repo.Datastore, node func() *core.IpfsNode, notify func(streamId string) error) *activeStreamStore {
	return &activeStreamStore{
		ctx:        ctx,
		datastore:  datastore,
		node:       node,
		streamList: make(map[string]*activeStream),
		notify:     notify,
	}
}

func (store *activeStreamStore) fileAsStream(sf *pb.StreamFile, file_type pb.StreamMeta_Type) (*pb.StreamMeta, error) {
	// r := bytes.NewReader(sf.Data)
	fi, err := os.Open(string(sf.Data))
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(fi)
	fileCid, err := ipfs.AddData(store.node(), r, true, false)
	fileid := fileCid.String()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	config := &pb.StreamMeta{
		Id:          fileid,
		Nsubstreams: 1,
		Caption:     string(sf.Description),
		Type:        file_type,
	}

	err = store.addStream(config)
	if err != nil {
		return nil, err
	}

	as := store.streamList[fileid]
	err = as.traverseNode(fileCid, true, sf.Description)
	if err != nil {
		return nil, err
	}
	err = as.handleFileEndmark()
	if err != nil {
		return nil, err
	}
	err = as.notify(as.meta.Id)
	if err != nil {
		return nil, err
	}

	//====== send notification to self
	record := &pb.Notification{
		Block:   config.Id,
		Date:    ptypes.TimestampNow(),
		Actor:   "",                           // self id. filled with "" if can not get.
		Subject: recorder.Event_ThreadAddFile, // event type
		Target:  "",                           // self id. The peer that add the file would be collector
		Body:    fileid,
		Read:    true, // send to notification channel. There is other notification fot thread add file.
	}
	recorder.RecordCh <- record
	//======
	return config, nil
}

func (store *activeStreamStore) isActive(id string) bool {
	store.lock.Lock()
	defer store.lock.Unlock()
	for _, s := range store.streamList {
		if s.meta.Id == id {
			return true
		}
	}
	return false
}

func (store *activeStreamStore) addStream(meta *pb.StreamMeta) error {
	log.Debugf("New stream %s add to activeStreamStore.", meta.Id)
	store.lock.Lock()
	defer store.lock.Unlock()
	_, ok := store.streamList[meta.Id]
	if ok {
		return &ErrStreamAlreadyActive{meta: meta}
	}
	newCtx, cancelFunc := context.WithCancel(store.ctx)
	newActiveStream := &activeStream{
		meta:         meta,
		datastore:    store.datastore,
		node:         store.node,
		fileChan:     make(chan *pb.StreamFile, 10),
		currentIndex: 0,
		ctx:          newCtx,
		cancel:       cancelFunc,
		notify:       store.notify,
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
		return &ErrStreamNotActive{streamId: streamId}
	}
	s.fileChan <- nil //use nil to represent the end mark
	//s.cancel()
	delete(store.streamList, streamId)
	return nil
}

func (store *activeStreamStore) streamAddFile(streamId string, file *pb.StreamFile) error {
	log.Debugf("Add file to active stream %s.", streamId)
	store.lock.Lock()
	defer store.lock.Unlock()
	s, ok := store.streamList[streamId]
	if !ok {
		return &ErrStreamNotActive{streamId: streamId}
	}
	s.fileChan <- file
	return nil
}

//func (store *activeStreamStore)

type activeStream struct {
	meta         *pb.StreamMeta
	datastore    repo.Datastore
	node         func() *core.IpfsNode
	fileChan     chan *pb.StreamFile
	currentIndex uint64
	cancel       context.CancelFunc
	notify       func(streamId string) error
	ctx          context.Context
}

func (as *activeStream) start() {
	defer log.Debugf("[%s] Stream %s", TAG_STREAMEND, as.meta.Id)
	log.Debugf("[%s] Stream %s", TAG_STREAMSTART, as.meta.Id)
Loop:
	for {
		select {
		case f := <-as.fileChan:
			err := as.handleNewFile(f)
			if err != nil {
				log.Error(err)
			}
		case <-as.ctx.Done():
			err := as.ctx.Err()
			if err != nil {
				log.Error(err)
			}

			close(as.fileChan)
			break Loop
		}
	}
}

/*
 * handle adding a new file to a stream
 * f.Data: the original content of the file
 * f.Description: meta data of the file (json), such as the filename
 */
func (as *activeStream) handleNewFile(f *pb.StreamFile) error {
	if f == nil {
		// handle the end mark
		return as.handleFileEndmark()
	} else {
		r := bytes.NewReader(f.Data)
		fileid, err := ipfs.AddData(as.node(), r, true, false)
		if err != nil {
			log.Error(err)
		}
		err = as.traverseNode(fileid, true, f.Description)
		if err != nil {
			return err
		}
		err = as.notify(as.meta.Id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (as *activeStream) handleFileEndmark() error {
	err := as.datastore.StreamMetas().UpdateNblocks(as.meta.Id, as.currentIndex+1)
	if err != nil {
		log.Error(err)
		return err
	}
	err = as.datastore.StreamBlocks().Add(&pb.StreamBlock{
		Id:          "",
		Streamid:    as.meta.Id,
		Index:       as.currentIndex,
		Size:        0,
		IsRoot:      true,
		Description: "",
	})
	if err != nil {
		log.Error(err)
		return err
	}
	err = as.notify(as.meta.Id)
	if err != nil {
		return err
	} // notify the workers to send the ENDMARK
	as.cancel()
	return nil
}

func (as *activeStream) traverseNode(cid *cid.Cid, isRoot bool, payload []byte) error {
	links, err := ipfs.LinksAtPath(as.node(), cid.String())
	if err != nil {
		return err
	}
	if len(links) == 0 {
		err = as.saveBlock(cid, isRoot, payload)
		if err != nil {
			return err
		}
	} else {
		for _, l := range links {
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

	desc := make(map[string]string)
	if isRoot {
		err = json.Unmarshal(payload, &desc)
		if err != nil {
			log.Warning(err)
			desc = make(map[string]string)
		}
	}
	desc["CID"] = cid.String()
	desc_json, _ := json.Marshal(desc)

	err = as.datastore.StreamBlocks().Add(&pb.StreamBlock{
		Id:          cid.String(),
		Streamid:    as.meta.Id,
		Index:       index,
		Size:        int32(stat.CumulativeSize),
		IsRoot:      isRoot,
		Description: string(desc_json),
	})
	if err != nil {
		log.Error(err)
		return err
	}
	as.currentIndex += 1
	return nil
}
