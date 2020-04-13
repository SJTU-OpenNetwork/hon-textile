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
