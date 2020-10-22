package mobile

// func (m *Mobile) SyncFile(file []byte) error {
// 	if !m.node.Started() {
// 		return core.ErrStopped
// 	}

// 	model := new(pb.SyncFile)
// 	if err := proto.Unmarshal(file, model); err != nil {
// 		return err
// 	}
//     return m.node.SyncFile(model)
// }

// func (m *Mobile) AddSyncFile(file []byte) error {
// 	if !m.node.Started() {
// 		return core.ErrStopped
// 	}

// 	model := new(pb.SyncFile)
// 	if err := proto.Unmarshal(file, model); err != nil {
// 		return err
// 	}
//     return m.node.AddSyncFile(model)
// }

// func (m *Mobile) PublishSyncFile(file []byte) error {
// 	if !m.node.Started() {
// 		return core.ErrStopped
// 	}

// 	model := new(pb.SyncFile)
// 	if err := proto.Unmarshal(file, model); err != nil {
// 		return err
// 	}
//     return m.node.PublishSyncFile(model)
// }

// func (m *Mobile) ListSyncFile(address string, fileType int32) ([]byte, error) {
// 	if !m.node.Started() {
// 		return nil, core.ErrStopped
// 	}

// 	files := m.node.ListSyncFile(address, pb.SyncFile_Type(fileType))
// 	return proto.Marshal(files)
// }

// func (m *Mobile) SearchSyncFiles(query []byte, options []byte) (*SearchHandle, error) {
// 	if !m.node.Online() {
// 		return nil, core.ErrOffline
// 	}

// 	mquery := new(pb.SyncFileQuery)
// 	if err := proto.Unmarshal(query, mquery); err != nil {
// 		return nil, err
// 	}
// 	moptions := new(pb.QueryOptions)
// 	if err := proto.Unmarshal(options, moptions); err != nil {
// 		return nil, err
// 	}

// 	resCh, errCh, cancel, err := m.node.SearchSyncFiles(mquery, moptions)
//     if err != nil {
//         log.Warning(err)
// 		return nil, err
// 	}
// 	return m.handleSearchStream(resCh, errCh, cancel)
// }
