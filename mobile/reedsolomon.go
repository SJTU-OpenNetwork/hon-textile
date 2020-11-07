package mobile

import (
	"github.com/SJTU-OpenNetwork/hon-textile/reedsolomon"
	"io"
)

type ShardList interface {
	ShardNumber() int
	ParityNumber() int
}

func Encode(reader io.Reader, size int) (ShardList, error){
	return reedsolomon.Encode(reader, size)
}
