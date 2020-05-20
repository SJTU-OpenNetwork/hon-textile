package recorder

// Use several global const variables to keep event type used in field subject in notification.
const (
	Event_Final = "final"
	Event_ThreadAddFile = "threadAddFile"	// used in core/thread_files.go/Thread.AddFiles()
	Event_CallIPFSGet = "ipfsGet"			// used in core/thread_files.go/Thread.handleFileBlock()
	Event_DoneIPFSPin = "ipfsPin"
	Event_DoneTextileProcess = "textileProcess"
	Event_DoneIPFSGet = "ipfsDone"			// used in core/thread_files.go/Thread.handleFileBlock()
)
