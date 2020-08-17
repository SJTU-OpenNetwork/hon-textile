package shadow

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	"github.com/libp2p/go-libp2p-core/peer"
	"net"
	"time"
)

const connectTimeout = 5 * time.Second

// Used to connect to shadow peer using tcp dial
func ConnectShadow(shadowIp string, shadowPort int, node *core.IpfsNode) error{
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return err
	}

	pi, err := askShadowInfo(shadowIp, shadowPort)
	if err != nil {
		log.Error("Error when ask shadow info: ", err)
		return err
	}

	newCtx, cancel := context.WithTimeout(node.Context(), connectTimeout)
	defer cancel()

	err = api.Swarm().Connect(newCtx, *pi)
	if err != nil {
		log.Error("Error when swarm connect shadow: ", err)
		return err
	} else {
		return nil
	}
}

type shadowCommand struct {
	Type string `json:"type"`
	Args []string `json:"args"`
}

func marshalCommand(tmpcmd *shadowCommand) (string, error) {
	//fmt.Printf("Try to marshal %s command\n", tmpcmd.Type)
	js, err := json.Marshal(tmpcmd)
	if err != nil {
		return "", err
	}
	return string(js), nil
}

// openRw open a bufio.ReadWriter to addr
func openRw(addr string) (*bufio.ReadWriter, error) {
	//fmt.Println("Dial " + addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Error("Error when dial shadow: ", err)
		return nil, errors.New("dial "+addr+" failed")
	}
	return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
}

func askShadowInfo(ip string, port int) (*peer.AddrInfo, error) {
	command := &shadowCommand{
		Type: "peerInfo",
		Args: nil,
	}
	commandStr, err := marshalCommand(command)
	if err != nil {
		log.Error("Error when marshal shadow command: ", err)
		return nil, err
	}
	rw, err := openRw(fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		log.Error("Error when open ReadWriter to shadow: ", err)
		return nil, err
	}

	_, err = rw.WriteString(commandStr+"\n")
	if err != nil {
		log.Error("Error when write to ReadWriter: ", err)
		return nil, err
	}

	err = rw.Flush()
	if err != nil {
		log.Error("Error when flush ReadWriter: ", err)
		return nil, err
	}

	response, err := rw.ReadString('\n')
	if err != nil {
		log.Error("Error occurs when read from ReadWriter: ", err)
		return nil, err
	}

	res := new(peer.AddrInfo)
	err = json.Unmarshal([]byte(response), res)
	if err != nil {
		log.Error("Error when unmarshal response from shadow: ", err)
		return nil, err
	}

	return res, nil
}
