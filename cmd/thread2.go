package cmd

import "net/http"

func Thread2List() error {
	res, err := executeJsonCmd(http.MethodGet, "thread2", params{}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}
