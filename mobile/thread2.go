package mobile

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/core"
	"github.com/textileio/go-threads/core/thread"
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
func (m *Mobile) Addthread2_Byte(threadid interface{}, bytes []byte) error {
	tid,ok := threadid.(thread.ID)
	if !ok {
		fmt.Println("Error for assertion")
		return nil
	}
	err := m.node.Thread2AddBytes(tid, bytes)
	if err != nil {
		return err
	}
	//Whether need flushcafe ?
//	m.node.FlushCafes()
	return nil
}