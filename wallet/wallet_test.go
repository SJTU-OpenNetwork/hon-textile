package wallet_test

import (
	"fmt"
	"testing"

	. "github.com/SJTU-OpenNetwork/hon-textile/wallet"
)

func TestHuaweiId(t *testing.T) {
    openid := "aaaabbbbccccddddeeeeffffgggghhhhaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    _, err := WalletFromHuaweiOpenId(openid)
    if (err != nil){
        fmt.Println(err)
    }
}

