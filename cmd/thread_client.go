package cmd

import (
	"net/http"
)

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

func ThreadClientAddString(threadId string, text string) error {
	cmdOpt := map[string]string{"threadId": threadId, "text": text}
	res, err := executeStringCmd(http.MethodPost, "threadClient/addString", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}