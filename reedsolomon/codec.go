package reedsolomon

import (
	//"github.com/klauspost/reedsolomon"
	//"os"
	//"io"
	"io"
)

type ShardList struct {
	shardNumber int
	parityNumber int
}

func (s *ShardList) ShardNumber() int {
	return s.shardNumber
}

func (s *ShardList) ParityNumber() int {
	return s.parityNumber
}

func Encode(reader io.Reader, size int) (*ShardList, error) {

	return nil, nil
}
