package cmd

import "net/http"

func ThreadClientAddGroup() error {
	res, err := executeJsonCmd(http.MethodPost, "threadClient/addGroup", params{}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}