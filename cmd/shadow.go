package cmd

import "net/http"

func ShadowStat() error {
	res, err := executeStringCmd(http.MethodGet, "shadow/stat", params{})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func SetServePeer(pubkey string) error {
	cmdOpt := map[string]string{"pubkey": pubkey}
	res, err := executeStringCmd(http.MethodPost, "shadow/setservepeer", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}