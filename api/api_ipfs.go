package api

import (
	"fmt"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/mr-tron/base58/base58"
	"github.com/SJTU-OpenNetwork/hon-textile/crypto"
	"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
)

// ipfsId godoc
// @Summary Get IPFS peer ID
// @Description Displays underlying IPFS peer ID
// @Tags ipfs
// @Produce text/plain
// @Success 200 {string} string "peer id"
// @Failure 500 {string} string "Internal Server Error"
// @Router /ipfs/id [get]
func (a *Api) ipfsId(g *gin.Context) {
	pid, err := a.Node.PeerId()
	if err != nil {
		a.abort500(g, err)
		return
	}
	g.String(http.StatusOK, pid.Pretty())
}

// ipfsSwarmConnect godoc
// @Summary Opens a new direct connection to a peer address
// @Description Opens a new direct connection to a peer using an IPFS multiaddr
// @Tags ipfs
// @Produce application/json
// @Param X-Textile-Args header string true "peer address"
// @Success 200 {array} string "ok"
// @Failure 400 {string} string "Bad Request"
// @Failure 500 {string} string "Internal Server Error"
// @Router /ipfs/swarm/connect [post]
func (a *Api) ipfsSwarmConnect(g *gin.Context) {
	args, err := a.readArgs(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	if len(args) == 0 {
		g.String(http.StatusBadRequest, "missing peer multi address")
		return
	}

	res, err := ipfs.SwarmConnect(a.Node.Ipfs(), []string{args[0]})
	if err != nil {
		g.String(http.StatusBadRequest, err.Error())
		return
	}

	g.JSON(http.StatusOK, res)
}

// ipfsSwarmPeers godoc
// @Summary List swarm peers
// @Description Lists the set of peers this node is connected to
// @Tags ipfs
// @Produce application/json
// @Param X-Textile-Opts header string false "verbose: Display all extra information, latency: Also list information about latency to each peer, streams: Also list information about open streams for each peer, direction: Also list information about the direction of connection" default(verbose="false",latency="false",streams="false",direction="false")
// @Success 200 {object} ipfs.ConnInfos "connection"
// @Failure 400 {string} string "Bad Request"
// @Failure 500 {string} string "Internal Server Error"
// @Router /ipfs/swarm/peers [get]
func (a *Api) ipfsSwarmPeers(g *gin.Context) {
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	verbose := opts["verbose"] == "true"
	latency := opts["latency"] == "true"
	streams := opts["streams"] == "true"
	direction := opts["direction"] == "true"

	res, err := ipfs.SwarmPeers(a.Node.Ipfs(), verbose, latency, streams, direction)
	if err != nil {
		g.String(http.StatusBadRequest, err.Error())
		return
	}

	g.JSON(http.StatusOK, res)
}

// ipfsCat godoc
// @Summary Cat IPFS data
// @Description Displays the data behind an IPFS CID (hash) or Path
// @Tags ipfs
// @Produce application/octet-stream
// @Param path path string true "ipfs/ipns cid"
// @Param X-Textile-Opts header string false "key: Key to decrypt data on-the-fly" default(key=)
// @Success 200 {array} byte "data"
// @Failure 400 {string} string "Bad Request"
// @Failure 401 {string} string "Unauthorized"
// @Failure 404 {string} string "Not Found"
// @Failure 500 {string} string "Internal Server Error"
// @Router /ipfs/cat/{path} [get]
func (a *Api) ipfsCat(g *gin.Context) {
	pth := g.Param("path")
	if pth == "" {
		g.String(http.StatusBadRequest, "Missing IPFS CID")
	}

	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}

	data, err := ipfs.DataAtPath(a.Node.Ipfs(), pth[1:])
	if err != nil {
		g.String(http.StatusNotFound, err.Error())
		return
	}

	var plaintext []byte
	if opts["key"] != "" {
		key, err := base58.Decode(opts["key"])
		if err != nil {
			g.String(http.StatusBadRequest, err.Error())
		}
		plaintext, err = crypto.DecryptAES(data, key)
		if err != nil {
			g.String(http.StatusUnauthorized, err.Error())
		}
	} else {
		plaintext = data
	}

	g.Data(http.StatusOK, "application/octet-stream", plaintext)
}

// pin the node at cid.
// this is used to test the functionality of some data distribute algorithm.
func (a *Api) ipfsPinCid(g *gin.Context) {

	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	path := opts["path"]
	nd, err := ipfs.NodeAtPath(a.Node.Ipfs(), path, ipfs.CatTimeout) //One minute at most to fetch the node.
	if err != nil {
		a.abort500(g, err)
		return
	}
	nodeSize, err := nd.Size()
	if err != nil {
		log.Error("Fail to get size of node %s", path)
	} else {
		log.Debugf("Fetch ipld node with size %v", nodeSize)
	}

	err = ipfs.PinNode(a.Node.Ipfs(), nd, true)	// Pin node recursively
	if err != nil {
		a.abort500(g, err)
		return
	}
	log.Debugf("Node at %s pinned recursively.", path)
	g.Status(http.StatusOK)
}

func (a *Api) ipfsCompare(g *gin.Context){
	opts, err := a.readOpts(g)
	fmt.Println("opts:",opts)
	if err != nil {
		a.abort500(g, err)
		return
	}
	path1 := opts["path1"]
	path2 := opts["path2"]
	n1,n2,same,sizeA_B, sizeB_A,err := ipfs.ComparePath(a.Node.Ipfs(),path1,path2)
	fmt.Printf("n1:%d, n2:%d, same:%d, n2-same:%d, sizeA-B;%d, sizeB-A:%d\n",n1,n2,same,n2-same,sizeA_B,sizeB_A)
}

func (a *Api) ipfsStatObject(g *gin.Context) {
	opts, err := a.readOpts(g)
	fmt.Println("opts:",opts)
	if err != nil {
		a.abort500(g, err)
		return
	}
	path := opts["path"]
	ipfs.StatObject(a.Node.Ipfs(), path)
}

func (a *Api) ipfsListCids(g *gin.Context){
	opts, err := a.readOpts(g)
	if err != nil {
		a.abort500(g, err)
		return
	}
	cid := opts["cid"]
	outPath := opts["out"]
	list, err := ipfs.ListCids(a.Node.Ipfs(), cid)
	if err != nil {
		a.abort500(g, err)
		return
	}
	if outPath != "" {
		log.Debugf("Output cid list to %s", outPath)
		_, err = os.Stat(outPath)
		if os.IsNotExist(err) {
			// Write list to file
			f, err := os.Create(outPath)
			if err != nil {
				log.Error()
			} else {
				for _, l := range list {
					_, err = f.WriteString(l + "\n")
					if err != nil {
						log.Error(err)
					}
				}
				err = f.Close()
				if err != nil {
					log.Error(err)
				}
			}
		} else if err != nil {
			log.Error(err)
		}
	}
	g.JSON(http.StatusOK, list)
}
