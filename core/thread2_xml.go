package core

import (
	"context"
	"encoding/xml"
	"fmt"
	cbornode "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
	"github.com/textileio/go-threads/core/thread"
)

const (
	TEXT_XML_MSG = 1
	IMG_XML_MSG =2
	VIDEO_XML_MSG =3
	STREAMMETA_XML_MSG =4

)
type XmlMsg struct {
	Type string
	Data []byte
}

/*
	ThreadAddStream

	ThreadAddText

	ThreadAddImg

	ThreadAddVideo
 */


//Add any type file to a thread
func (t *Textile) Thread2AddFile(id interface{}, msgtype string, data []byte) error {
	xmlmsg := &XmlMsg{Type:msgtype,Data:data}
	output, err := xml.Marshal(xmlmsg)
	if err != nil{
		return err
	}
	tid,ok := id.(thread.ID)
	if !ok {
		fmt.Println("Error for assertion")
		return nil
	}
	body, err := cbornode.WrapObject(output, mh.SHA2_256, -1)
	if err != nil {
		return err
	}
	mctx, cancel := context.WithTimeout(t.ctx, msgTimeout)
	defer cancel()
	if _, err := t.thread2.net.CreateRecord(mctx, tid, body); err != nil {
		return err
	}
	return nil
}
