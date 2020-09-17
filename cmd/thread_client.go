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

func ThreadClientAddDB() error {
	res, err := executeJsonCmd(http.MethodPost, "threadClient/addDB", params{}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientListDB() error {
	res, err := executeJsonCmd(http.MethodPost, "threadClient/listDB", params{}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}