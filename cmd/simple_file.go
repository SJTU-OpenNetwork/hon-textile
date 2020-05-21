package cmd

import "net/http"

func AddSimpleFile(path string, threadId string) error {
	cmdOpt := map[string]string{"path": path, "threadId": threadId}
	res, err := executeStringCmd(http.MethodPost, "simpleFile/add", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}
