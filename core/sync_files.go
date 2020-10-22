package core

/** Bug exists here
 * If one device connects one cafe when adding but connects another
 * when removing, the removed file is still here.
 */

// func (t *Textile) SyncFile(file *pb.SyncFile) error {
// 	err := t.datastore.SyncFiles().Add(file)
// 	if err != nil {
// 		return err
// 	}
// 	return t.PublishSyncFile(file)
// }

// func (t *Textile) AddSyncFile(file *pb.SyncFile) error {
// 	return t.datastore.SyncFiles().Add(file)
// }

// func (t *Textile) PublishSyncFile(file *pb.SyncFile) error {
// 	sessions := t.datastore.CafeSessions().List().Items
// 	if len(sessions) == 0 {
// 		return nil
// 	}
// 	for _, session := range sessions {
// 		if err := t.cafe.PublishSyncFile(file, session.Id); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// // SearchVideo searches the network for a video
// func (t *Textile) SearchSyncFiles(query *pb.SyncFileQuery, options *pb.QueryOptions) (<-chan *pb.QueryResult, <-chan error, *broadcast.Broadcaster, error) {
// 	payload, err := proto.Marshal(query)
// 	if err != nil {
// 		return nil, nil, nil, err
// 	}

// 	options.Filter = pb.QueryOptions_HIDE_OLDER

// 	resCh, errCh, cancel := t.search(&pb.Query{
// 		Type:    pb.Query_SYNC_FILE,
// 		Options: options,
// 		Payload: &any.Any{
// 			TypeUrl: "/SyncFileQuery",
// 			Value:   payload,
// 		},
// 	})
// 	return resCh, errCh, cancel, nil
// }

// func (t *Textile) ListSyncFile(address string, fileType pb.SyncFile_Type) *pb.SyncFileList {
// 	files := &pb.SyncFileList{Items: make([]*pb.SyncFile, 0)}
// 	for _, c := range t.datastore.SyncFiles().ListByType(address, fileType) {
// 		files.Items = append(files.Items, c)
// 	}
// 	return files
// }
