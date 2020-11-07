package mobile

import (
	"github.com/SJTU-OpenNetwork/hon-textile/reedsolomon"
	"io"
)

type ShardList interface {
	ShardNumber() int
	ParityNumber() int
}

func Encode(reader io.Reader) (ShardList, error){
	return reedsolomon.Encode(reader)
}
