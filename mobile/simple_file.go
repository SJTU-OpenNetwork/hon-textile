package mobile

// AddSimpleFile aims to simplify the add file process
// The process is supposed to be really simple:
//		- Add file to ipfs and get the corresponding file cid.
//		- Add cid to thread. DO NOT BUILD IPLD NODE FOR IT.
//		- Get the file through ipfs.get(cid) when receive a simplefile thread block.
// Note:
//		- No encryption
//		- The time used for each step would be kept and sent by record_service.
func (m *Mobile) AddSimpleFile(path string, threadId string, cb ProtoCallback) {
	m.node.WaitAdd(1, "Mobile.AddFiles")
	go func() {
		defer m.node.WaitDone("Mobile.AddFiles")

		resolvedPath, err := m.node.AddSimpleFile(path, threadId)
		if err != nil {
			cb.Call(nil, err)
			return
		}

		cb.Call(resolvedPath.Cid().Bytes(), nil)
	}()
}

