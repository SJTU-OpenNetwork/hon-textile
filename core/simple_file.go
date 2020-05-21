package core

//import "github.com/SJTU-OpenNetwork/hon-textile/ipfs"

import (
	//"bufio"
	"github.com/SJTU-OpenNetwork/go-ipfs/core/coreapi"
	"github.com/SJTU-OpenNetwork/interface-go-ipfs-core/options"
	ipfspath "github.com/SJTU-OpenNetwork/interface-go-ipfs-core/path"
	files "github.com/ipfs/go-ipfs-files"
	"os"
)
// Add file to ipfs node
// Return hash
// Note:
//		AddSimpleFile does the same level task as Textile.AddFileIndex and thread.AddFile
//		But it has nothing to do with Schema, Mill, and Thread.
//		And file added through AddSimpleFile would not be written in FileStore
//		Besides, instead of calling hon-textile/ipfs.AddData, AddSimpleFile use ipfs/coreapi directly to add file.
func (t *Textile) AddSimpleFile(path string, threadId string) (ipfspath.Resolved, error){
	//hash, err := ipfs.AddData(t.node, reader, mill.Pin(), false)
	api, err := coreapi.NewCoreAPI(t.node)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	thread := t.Thread(threadId)
	if thread == nil {
		return nil, ErrThreadNotFound
	}

	// Open file and get reader for file
	fi, err := os.Open(path)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer func() {
		err := fi.Close()
		if err != nil {log.Error(err)}
	}()

	// Add file to ipfs
	resolvedPath, err := api.Unixfs().Add(t.ctx, files.NewReaderFile(fi), options.Unixfs.HashOnly(false), options.Unixfs.Chunker("size-1048576"))

	// Add file to thread

	return resolvedPath, err
}
