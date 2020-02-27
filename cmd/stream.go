package cmd

import (
	"fmt"
	"net/http"
	"strconv"
)

func StreamCreate(streamId string, subNum int) error {
	fmt.Printf("Call cmd/stream.go/StreamCreate")
	cmdOpt := map[string]string{"streamId": streamId, "subNum": strconv.Itoa(subNum)}
	res, err := executeStringCmd(http.MethodPost, "stream/create", params{opts: cmdOpt})
	if err != nil {
		return err
	}
	output(res)

	return nil
}
