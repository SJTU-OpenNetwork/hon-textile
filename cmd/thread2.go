package cmd

import (
	"net/http"
	"strings"
)

func Thread2List() error {
	res, err := executeJsonCmd(http.MethodGet, "thread2", params{}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func Thread2Create() error {
	res, err := executeJsonCmd(http.MethodPost, "thread2/create", params{}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func Thread2AddString(threadId string, text string) error {
	res, err := executeJsonCmd(http.MethodPut, "thread2/addString/"+threadId, params{
		payload: strings.NewReader(text)}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}
