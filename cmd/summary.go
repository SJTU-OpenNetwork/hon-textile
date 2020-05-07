package cmd

import (
	"net/http"

	"github.com/SJTU-OpenNetwork/hon-textile/pb"
)

func Summary() error {
	var info pb.Summary
	res, err := executeJsonPbCmd(http.MethodGet, "summary", params{}, &info)
	if err != nil {
		return err
	}
	output(res)
	return nil
}
