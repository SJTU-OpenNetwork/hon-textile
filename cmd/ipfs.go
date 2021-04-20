package cmd

import (
	"fmt"
	"net/http"
	"strconv"
)

func IpfsPeer() error {
	res, err := executeStringCmd(http.MethodGet, "ipfs/id", params{})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func IpfsSwarmConnect(address string) error {
	res, err := executeJsonCmd(http.MethodPost, "ipfs/swarm/connect", params{
		args: []string{address},
	}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func IpfsSwarmPeers(verbose bool, streams bool, latency bool, direction bool) error {
	res, err := executeJsonCmd(http.MethodGet, "ipfs/swarm/peers", params{
		opts: map[string]string{
			"verbose":   strconv.FormatBool(verbose),
			"streams":   strconv.FormatBool(streams),
			"latency":   strconv.FormatBool(latency),
			"direction": strconv.FormatBool(direction),
		},
	}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func IpfsCat(hash string, key string) error {
	return executeBlobCmd(http.MethodGet, "ipfs/cat/"+hash, params{
		opts: map[string]string{"key": key},
	})
}


func IpfsPinCid(path string) error {
	res, err := executeStringCmd(http.MethodPost, "ipfs/pin", params{
		opts: map[string]string{"path":path},
	})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func IpfsListCids(cid string, outPath string) error {
	res, err := executeStringCmd(http.MethodGet, "ipfs/listcids", params{
		//cid := opts["cid"]
		//outPath := opts["out"]
		opts: map[string]string{"cid":cid, "out": outPath},
	})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func IpfsCompare(path1 string, path2 string) error{
	fmt.Println("compare")
	res,err:=executeStringCmd(http.MethodGet, "ipfs/compare", params{
		opts: map[string]string{"path1": path1, "path2":path2},
	})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func IpfsStatObject(cid string) error {
	res, err := executeStringCmd(http.MethodGet, "ipfs/stat", params{
		opts: map[string]string{"path":cid},
	})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

