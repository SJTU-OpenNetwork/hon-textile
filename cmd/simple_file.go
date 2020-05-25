package cmd

import "net/http"

func AddSimpleFile(path string, threadId string) error {
	cmdOpt := map[string]string{"path": path, "threadId": threadId}
	res, err := executeJsonCmd(http.MethodPost, "simpleFile/add", params{opts:cmdOpt}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}
