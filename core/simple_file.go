package core

//import "github.com/SJTU-OpenNetwork/hon-textile/ipfs"

import (
	"bufio"
	"errors"

	"os"

	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
)

func (t *Textile) AddSimpleFile(path string, threadId string) (*pb.Block, error) {
	return t.addSimpleFile(path, threadId, pb.SimpleFile_FILE)
}

func (t *Textile) AddSimplePicture(path string, threadId string) (*pb.Block, error) {
	return t.addSimpleFile(path, threadId, pb.SimpleFile_PICTURE)
}

// Add file to ipfs node
// Return hash
// Note:
//		AddSimpleFile does the same level task as Textile.AddFileIndex and thread.AddFile
//		But it has nothing to do with Schema, Mill, and Thread.
//		And file added through AddSimpleFile would not be written in FileStore
//		Besides, instead of calling hon-textile/ipfs.AddData, AddSimpleFile use ipfs/coreapi directly to add file.
func (t *Textile) addSimpleFile(path string, threadId string, fileType pb.SimpleFile_Type) (*pb.Block, error) {
	//hash, err := ipfs.AddData(t.node, reader, mill.Pin(), false)
	log.Debugf("AddSimpleFile(%s, %s)", path, threadId)

	thread := t.Thread(threadId)
	if thread == nil {
		return nil, ErrThreadNotFound
	}

	// Open file and get reader for file
	fileInfo, err := os.Stat(path)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if fileInfo.IsDir() {
		err = errors.New("SimpleFile does not support directory")
		log.Error(err)
		return nil, err
	}

	fi, err := os.Open(path)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer func() {
		err := fi.Close()
		if err != nil {
			log.Error(err)
		}
	}()

	// Add file to ipfs
	r := bufio.NewReader(fi)
	fileCid, err := ipfs.AddData(t.node, r, true, false)
	// resolvedPath, err := api.Unixfs().Add(t.ctx, files.NewReaderFile(fi), options.Unixfs.HashOnly(false), options.Unixfs.Chunker("size-1048576"))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	// Add file to thread
	return thread.AddSimpleFile(&pb.SimpleFile{
		Name: fileInfo.Name(),
		Path: fileCid.String(),
		Size: fileInfo.Size(),
		Type: fileType,
	})
}
