package mobile
import "github.com/golang/protobuf/proto"
// AddSimpleFile aims to simplify the add file process
// The process is supposed to be really simple:
//		- Add file to ipfs and get the corresponding file cid.
//		- Add cid to thread. DO NOT BUILD IPLD NODE FOR IT.
//		- Get the file through ipfs.get(cid) when receive a simplefile thread block.
// Note:
//		- No encryption
//		- The time used for each step would be kept and sent by record_service.
func (m *Mobile) AddSimpleFile(path string, threadId string, cb ProtoCallback) {
	m.node.WaitAdd(1, "Mobile.AddSimpleFile")
	go func() {
		defer m.node.WaitDone("Mobile.AddSimpleFile")

		block, err := m.node.AddSimpleFile(path, threadId)
		if err != nil {
			cb.Call(nil, err)
			return
		}
		blockView, err := proto.Marshal(block)
		if err != nil {
			cb.Call(nil, err)
			return
		}
		m.node.FlushCafes()
		cb.Call(blockView, nil)
	}()
}

func (m *Mobile) AddSimplePicture(path string, threadId string, cb ProtoCallback) {
	m.node.WaitAdd(1, "Mobile.AddSimplePicture")
	go func() {
		defer m.node.WaitDone("Mobile.AddSimplePicture")

		block, err := m.node.AddSimplePicture(path, threadId)
		if err != nil {
			cb.Call(nil, err)
			return
		}
		blockView, err := proto.Marshal(block)
		if err != nil {
			cb.Call(nil, err)
			return
		}
		m.node.FlushCafes()
		cb.Call(blockView, nil)
	}()
}

func (m *Mobile) AddSimpleFolder(path string, threadId string, cb ProtoCallback){
	m.node.WaitAdd(1, "Mobile.AddSimplePicture")
	go func() {
		defer m.node.WaitDone("Mobile.AddSimplePicture")

		block, err := m.node.AddSimpleDirectory(path, threadId)
		if err != nil {
			cb.Call(nil, err)
			return
		}
		blockView, err := proto.Marshal(block)
		if err != nil {
			cb.Call(nil, err)
			return
		}
		m.node.FlushCafes()
		cb.Call(blockView, nil)
	}()
}
