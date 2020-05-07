package core

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
)

// handleMergeBlock handles an incoming merge block
// Deprecated
func (t *Thread) handleMergeBlock(block *pb.ThreadBlock) (handleResult, error) {
	var res handleResult

	if !t.readable(t.config.Account.Address) {
		return res, ErrNotReadable
	}
	if !t.readable(block.Header.Address) {
		return res, ErrNotReadable
	}

	return res, nil
}
