package mobile

import "github.com/SJTU-OpenNetwork/hon-textile/core"

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
