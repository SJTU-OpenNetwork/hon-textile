package cmd

import (
	"fmt"

	"github.com/SJTU-OpenNetwork/hon-textile/common"
)

func Version(git bool) error {
	if git {
		fmt.Println("go-textile version " + common.GitSummary)
	} else {
		fmt.Println("go-textile version v" + common.Version)
	}
	return nil
}
