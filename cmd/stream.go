package cmd

import (
	//"fmt"
	"net/http"
	"strconv"
)

func StreamCreate(threadId string, streamId string, subNum int) error {
	//fmt.Printf("Call cmd/stream.go/StreamCreate")
	cmdOpt := map[string]string{"threadId": threadId, "streamId": streamId, "subNum": strconv.Itoa(subNum)}
	res, err := executeStringCmd(http.MethodPost, "stream/create", params{opts: cmdOpt})
	if err != nil {
		return err
	}
	output(res)

	return nil
}

func StreamAddFile(streamId string, filePath string) error {
	cmdOpt := map[string]string{"streamId": streamId, "filePath": filePath}
	res, err := executeStringCmd(http.MethodPost, "stream/addfile", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)

	return nil
}

func StreamSubscribe(streamId string) error {
	cmdOpt := map[string]string{"streamId": streamId}
	res, err := executeStringCmd(http.MethodPost, "stream/subscribe", params{opts: cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func StreamWorkerStat() error {
	res, err := executeStringCmd(http.MethodGet, "stream/workerstat", params{})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func StreamClose(streamId string) error {
	//cmdArg := []string{streamId}
	cmdOpt := map[string] string{"stringId": streamId}
	res, err := executeStringCmd(http.MethodPost, "stream/close", params{opts:cmdOpt})
	if err != nil {
		return nil
	}
	output(res)
	return nil
}
