package mobile

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/core"
	thread2 "github.com/textileio/go-threads/core/thread"
)

/*
 Class xxxx implements Thread2Handler {
	@Override
	HandlerMsg() {
		xxxxxxx
	}
}
 */
type Thread2Handler interface {
	HandleMsg(msg *core.XmlMsg)
}

func Thread2Subscribe(handler Thread2Handler) {
	// core.Thread2Subscrtibe
}

//In backend, we will not use pb.StreamMeta, we directly receive the []byte.
func (m *Mobile) Thread2_AddFile(threadid string, ftype string, bytes []byte) error {
	threadId, err := thread2.Decode(threadid)
	if err != nil {
		fmt.Println("Error when thread2 decode string to thread.ID")
		return err
	}
	err = m.node.Thread2AddBytes(threadId, ftype, bytes)
	if err != nil {
		return err
	}
	return nil
}