package ipfs

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	ospath "path"
	"sort"
	"strings"
	"time"

	icid "github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	dag "github.com/ipfs/go-merkledag"
	uio "github.com/ipfs/go-unixfs/io"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

var log = logging.Logger("tex-ipfs")

const DefaultTimeout = time.Second * 2 //from 5 to 2 2019.11.27
const PinTimeout = 5 * time.Minute
const CatTimeout = 20 * time.Minute
const ConnectTimeout = time.Second * 5 //from 10 to 5 2019.11.27

func PutBlock(node *core.IpfsNode, src io.Reader) (iface.BlockStat, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), CatTimeout)
	defer cancel()

	return api.Block().Put(ctx, src, options.Block.Pin(true))
}

func GetBlock(node *core.IpfsNode, p path.Path) (io.Reader, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), CatTimeout)
	defer cancel()

	return api.Block().Get(ctx, p)
}

func FilePathAtIpfsPath(node *core.IpfsNode, pth string, repoPath string) (string, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(node.Context(), CatTimeout)
	defer cancel()

	f, err := api.Unixfs().Get(ctx, path.New(pth))
	if err != nil {
		log.Error(err)
		return "", err
	}
	defer f.Close()

	var file files.File
	switch f := f.(type) {
	case files.File:
		file = f
	case files.Directory:
		return "", iface.ErrIsDir
	default:
		return "", iface.ErrNotSupported
	}

	tmpFilesDir := ospath.Join(repoPath, "tmpfiles")
	_, err = os.Stat(tmpFilesDir)
	if os.IsNotExist(err) {
		os.Mkdir(tmpFilesDir, 0777)
		os.Chmod(tmpFilesDir, 0777)
	}

	tmpPath := ospath.Join(tmpFilesDir, pth)
	tmpFile, _ := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	defer tmpFile.Close()
	srcBuf := bufio.NewReader(file)
	desBuf := bufio.NewWriter(tmpFile)
	io.Copy(desBuf, srcBuf)
	desBuf.Flush()

	return tmpPath, nil
}

func FolderAtPath(node *core.IpfsNode, pth string, repoPath string) (string, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(node.Context(), CatTimeout)
	defer cancel()

	rootNodeDirectory, err := api.Unixfs().Get(ctx, path.New(pth))
	if err != nil {
		return "", err
	}

	fileFolder := ospath.Join(repoPath, pth)

    os.RemoveAll(fileFolder)
	err = files.WriteTo(rootNodeDirectory, fileFolder)

	if err != nil {
		return "", err
	}

	return fileFolder, nil
}

// DataAtPath return bytes under an ipfs path
func DataAtPath(node *core.IpfsNode, pth string) ([]byte, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), CatTimeout)
	defer cancel()

	f, err := api.Unixfs().Get(ctx, path.New(pth))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer f.Close()

	var file files.File
	switch f := f.(type) {
	case files.File:
		file = f
	case files.Directory:
		return nil, iface.ErrIsDir
	default:
		return nil, iface.ErrNotSupported
	}

	return ioutil.ReadAll(file)
}

// LinksAtPath return ipld links under a path
func LinksAtPath(node *core.IpfsNode, pth string) ([]*ipld.Link, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), CatTimeout)
	defer cancel()

	res, err := api.Unixfs().Ls(ctx, path.New(pth))
	if err != nil {
		return nil, err
	}

	links := make([]*ipld.Link, 0)
	for link := range res {
		links = append(links, &ipld.Link{
			Name: link.Name,
			Size: link.Size,
			Cid:  link.Cid,
		})
	}

	return links, nil
}

// AddDataToDirectory adds reader bytes to a virtual dir
func AddDataToDirectory(node *core.IpfsNode, dir uio.Directory, fname string, reader io.Reader) (*icid.Cid, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	id, err := AddData(node, reader, false, false)
	if err != nil {
		return nil, err
	}

	n, err := api.Dag().Get(node.Context(), *id)
	if err != nil {
		return nil, err
	}

	err = dir.AddChild(node.Context(), fname, n)
	if err != nil {
		return nil, err
	}

	return id, nil
}

// AddLinkToDirectory adds a link to a virtual dir
func AddLinkToDirectory(node *core.IpfsNode, dir uio.Directory, fname string, pth string) error {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return err
	}

	id, err := icid.Decode(pth)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(node.Context(), DefaultTimeout)
	defer cancel()

	nd, err := api.Dag().Get(ctx, id)
	if err != nil {
		return err
	}

	ctx2, cancel2 := context.WithTimeout(node.Context(), DefaultTimeout)
	defer cancel2()

	return dir.AddChild(ctx2, fname, nd)
}

// AddData takes a reader and adds it, optionally pins it, optionally only hashes it
func AddData(node *core.IpfsNode, reader io.Reader, pin bool, hashOnly bool) (*icid.Cid, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), PinTimeout)
	defer cancel()

	//pth, err := api.Unixfs().Add(ctx, files.NewReaderFile(reader), options.Unixfs.HashOnly(hashOnly), options.Unixfs.Chunker("size-1048576")) //size = 1M
	//pth, err := api.Unixfs().Add(ctx, files.NewReaderFile(reader), options.Unixfs.HashOnly(hashOnly), options.Unixfs.Chunker("rabin-65536-262144-1048576")) //size = 1M
	//pth, err := api.Unixfs().Add(ctx, files.NewReaderFile(reader), options.Unixfs.HashOnly(hashOnly), options.Unixfs.Chunker("honrabin-65536-1048576")) //size = 1M
	//pth, err := api.Unixfs().Add(ctx, files.NewReaderFile(reader), options.Unixfs.HashOnly(hashOnly), options.Unixfs.Chunker("ram-65536-1048576-4")) //size = 1M
	//pth, err := api.Unixfs().Add(ctx, files.NewReaderFile(reader), options.Unixfs.HashOnly(hashOnly), options.Unixfs.Chunker("newram2-65536-1048576-4")) //size = 1M
	pth, err := api.Unixfs().Add(ctx, files.NewReaderFile(reader), options.Unixfs.HashOnly(hashOnly), options.Unixfs.Chunker("hram-32768-1048576-4")) //size = 64k~1M
	//pth, err := api.Unixfs().Add(ctx, files.NewReaderFile(reader), options.Unixfs.HashOnly(hashOnly)) //size = 256K
	if err != nil {
		return nil, err
	}

	if pin && !hashOnly {
		err = api.Pin().Add(ctx, pth, options.Pin.Recursive(false))
		if err != nil {
			return nil, err
		}
	}
	id := pth.Cid()

	return &id, nil
}

// AddObject takes a reader and adds it as a DAG node, optionally pins it
func AddObject(node *core.IpfsNode, reader io.Reader, pin bool) (*icid.Cid, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), PinTimeout)
	defer cancel()

	pth, err := api.Object().Put(ctx, reader)
	if err != nil {
		return nil, err
	}

	if pin {
		err = api.Pin().Add(ctx, pth, options.Pin.Recursive(false))
		if err != nil {
			return nil, err
		}
	}
	id := pth.Cid()

	return &id, nil
}

func AddFolder(node *core.IpfsNode, path string, pin bool) (*icid.Cid, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	dir, err := getUnixfsNode(path)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), PinTimeout)
	defer cancel()

	//pth, err := api.Unixfs().Add(ctx, dir, , options.Unixfs.HashOnly(hashOnly))
	pth, err := api.Unixfs().Add(ctx, dir)
	if err != nil {
		return nil, err
	}

	//if pin && !hashOnly {
	if pin {
		err = api.Pin().Add(ctx, pth, options.Pin.Recursive(false))
		if err != nil {
			return nil, err
		}
	}
	id := pth.Cid()

	return &id, nil
}

func getUnixfsNode(path string) (files.Node, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	f, err := files.NewSerialFile(path, true, st)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// NodeAtLink returns the node behind an ipld link
func NodeAtLink(node *core.IpfsNode, link *ipld.Link) (ipld.Node, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), CatTimeout)
	defer cancel()
	return link.GetNode(ctx, api.Dag())
}

// NodeAtCid returns the node behind a cid
func NodeAtCid(node *core.IpfsNode, id icid.Cid) (ipld.Node, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), CatTimeout)
	defer cancel()
	return api.Dag().Get(ctx, id)
}

// NodeAtPath returns the last node under path
func NodeAtPath(node *core.IpfsNode, pth string, timeout time.Duration) (ipld.Node, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), timeout)
	defer cancel()

	return api.ResolveNode(ctx, path.New(pth))
}

type Node struct {
	Links []Link
	Data  string
}

type Link struct {
	Name, Hash string
	Size       uint64
}

// ObjectAtPath returns the DAG object at the given path
func ObjectAtPath(node *core.IpfsNode, pth string) ([]byte, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), CatTimeout)
	defer cancel()

	ipth := path.New(pth)
	nd, err := api.Object().Get(ctx, ipth)
	if err != nil {
		return nil, err
	}

	r, err := api.Object().Data(ctx, ipth)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	out := &Node{
		Links: make([]Link, len(nd.Links())),
		Data:  string(data),
	}

	for i, link := range nd.Links() {
		out.Links[i] = Link{
			Hash: link.Cid.String(),
			Name: link.Name,
			Size: link.Size,
		}
	}

	return json.Marshal(out)
}

// StatObjectAtPath returns info about an object
func StatObjectAtPath(node *core.IpfsNode, pth string) (*iface.ObjectStat, error) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(node.Context(), CatTimeout)
	defer cancel()

	return api.Object().Stat(ctx, path.New(pth))
}

// PinNode pins an ipld node
func PinNode(node *core.IpfsNode, nd ipld.Node, recursive bool) error {
	ctx, cancel := context.WithTimeout(node.Context(), PinTimeout)
	defer cancel()

	defer node.Blockstore.PinLock().Unlock()

	err := node.Pinning.Pin(ctx, nd, recursive)
	if err != nil {
		if strings.Contains(err.Error(), "already pinned recursively") {
			return nil
		}
		return err
	}

	return node.Pinning.Flush(ctx)
}

// UnpinNode unpins an ipld node
func UnpinNode(node *core.IpfsNode, nd ipld.Node, recursive bool) error {
	return UnpinCid(node, nd.Cid(), recursive)
}

// UnpinCid unpins a cid
func UnpinCid(node *core.IpfsNode, id icid.Cid, recursive bool) error {
	ctx, cancel := context.WithTimeout(node.Context(), PinTimeout)
	defer cancel()

	err := node.Pinning.Unpin(ctx, id, recursive)
	if err != nil {
		return err
	}

	return node.Pinning.Flush(ctx)
}

// Pinned returns the subset of given cids that are pinned
func Pinned(node *core.IpfsNode, cids []string) ([]icid.Cid, error) {
	var decoded []icid.Cid
	ctx, cancel := context.WithTimeout(node.Context(), DefaultTimeout)
	defer cancel()
	for _, id := range cids {
		dec, err := icid.Decode(id)
		if err != nil {
			return nil, err
		}
		decoded = append(decoded, dec)
	}
	list, err := node.Pinning.CheckIfPinned(ctx, decoded...)
	if err != nil {
		return nil, err
	}

	var pinned []icid.Cid
	for _, p := range list {
		if !p.Pinned() {
			pinned = append(pinned, p.Key)
		}
	}

	return pinned, nil
}

// NotPinned returns the subset of given cids that are not pinned
func NotPinned(node *core.IpfsNode, cids []string) ([]icid.Cid, error) {
	ctx, cancel := context.WithTimeout(node.Context(), DefaultTimeout)
	defer cancel()
	var decoded []icid.Cid
	for _, id := range cids {
		dec, err := icid.Decode(id)
		if err != nil {
			return nil, err
		}
		decoded = append(decoded, dec)
	}
	list, err := node.Pinning.CheckIfPinned(ctx, decoded...)
	if err != nil {
		return nil, err
	}

	var notPinned []icid.Cid
	for _, p := range list {
		if !p.Pinned() {
			notPinned = append(notPinned, p.Key)
		}
	}

	return notPinned, nil
}

// ResolveLinkByNames resolves a link in a node from a list of valid names
// Note: This exists for b/c w/ the "f" -> "meta" and "d" -> content migration
func ResolveLinkByNames(nd ipld.Node, names []string) (*ipld.Link, error) {
	for _, n := range names {
		link, _, err := nd.ResolveLink([]string{n})
		if err != nil {
			if err == dag.ErrLinkNotFound {
				continue
			}
			return nil, err
		}
		if link != nil {
			return link, nil
		}
	}
	return nil, nil
}

// traverse node according to cid and return all the cids belonging to it.
func ListCids(node *core.IpfsNode, cid string) ([]string, error) {
	// Fetch the node first
	nd, err := NodeAtPath(node, cid, CatTimeout) //One minute at most to fetch the node.
	if err != nil {
		log.Error(err)
		return nil, err
	}

	err = PinNode(node, nd, true) // Pin node recursively
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// Traverse node
	return traverseAndList(node, cid)
}

func ListSortCids(node *core.IpfsNode, cid string) ([]string, error) {
	list, err := traverseAndList(node, cid)
	if err != nil {
		log.Error("Error when traverse ", cid, ": ", err)
		return nil, err
	}
	sort.Strings(list)
	return list, nil
}

func traverseAndList(node *core.IpfsNode, cid string) ([]string, error) {
	res := []string{cid}
	links, err := LinksAtPath(node, cid)
	if err != nil {
		return nil, err
	}
	if len(links) == 0 {
		return res, nil
	} else {
		for _, l := range links {
			list, err := traverseAndList(node, l.Cid.String())
			if err != nil {
				return nil, err
			}
			res = append(res, list...)
		}
	}
	return res, nil
}

func traverseAndGetLeaf(node *core.IpfsNode, cid string) ([]string,error){
	res := []string{}
	links, err := LinksAtPath(node, cid)
	if err != nil {
		return nil, err
	}
	if len(links) == 0 {
		res = append(res,cid)
		return res, nil
	} else {
		for _, l := range links {
			list, err := traverseAndGetLeaf(node, l.Cid.String())
			if err != nil {
				return nil, err
			}
			res = append(res, list...)
		}
	}
	return res, nil
}

// Note that ComparePath would not fetch or pin the cid.
// return
//	- number of cids in first path
//	- number of cids in second path
// 	- number of same cids
//  - number of A minus B
//	- number of B minus A
//	- error
func ComparePath(node *core.IpfsNode, pth1 string, pth2 string) (int, int, int, int, int, error){
	list1, err := traverseAndGetLeaf(node, pth1)
	if err != nil {
		log.Error("Error when traverse node ", pth1, ": ", err)
		//recorder.Hlog.Add("Error when traverse node "+ pth1+": " + err.Error())
		return 0,0,0,0,0,err
	}
	//sort.Strings(list1)

	list2, err := traverseAndGetLeaf(node, pth2)
	if err != nil {
		log.Error("Error when traverse node ", pth2, ": ", err)
		//recorder.Hlog.Add("Error when traverse node "+ pth2+": " + err.Error())
		return 0,0,0,0,0,err
	}
	//sort.Strings(list2)

	n1 := len(list1)
	n2 := len(list2)
	if n1==0 || n2==0 {
		return 0, 0, 0, 0, 0, nil
	}
	AminusB:=[]string{}
	BminusA:=[]string{}
	var i,j int
	for i=0; i<n1; i++ {
		for j=0; j<n2; j++ {
			if strings.Compare(list1[i],list2[j])==0 {
				break
			}
		}
		if j==n2 {
			AminusB=append(AminusB,list1[i])
		}
	}
	i=0
	j=0
	for i=0; i<n2; i++ {
		for j=0; j<n1; j++ {
			if strings.Compare(list2[i],list1[j])==0 {
				break
			}
		}
		if j==n1 {
			BminusA=append(BminusA,list2[i])
		}
	}

	same:=(n1+n2-len(AminusB)-len(BminusA))/2

	var bytes []byte
	dataSizeAminusB :=0
	dataSizeBminusA :=0
	for i=0; i<len(AminusB); i++ {
		bytes,_=DataAtPath(node, AminusB[i])
		dataSizeAminusB+=len(bytes)
	}
	for i=0; i<len(BminusA); i++ {
		bytes,_= DataAtPath(node, BminusA[i])
		dataSizeBminusA+=len(bytes)
	}

	//n1 := len(list1)
	//n2 := len(list2)
	//var same = 0
	//var i1 = 0
	//var i2 = 0
	//for i1<n1 && i2 < n2 {
	//	if list1[i1] == list2[i2] {
	//		same +=1
	//		i1+=1
	//		i2+=1
	//	} else if list1[i1] < list2[i2] {
	//		i1+=1
	//	} else {
	//		i2+=1
	//	}
	//}

	return n1, n2, same, dataSizeAminusB, dataSizeBminusA, nil
}
