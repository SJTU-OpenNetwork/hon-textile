package recorder

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"sync"
)

// Refer to record_service.go to see how the fields of notification is used to keep record.
// Notifer is used to manage a series of event records, such as the adding process when add a file to thread.
// All these event notifications may have the same "block" and "target" field.
type Notifer struct {
	id string	// the "block" field of notifications
	target string // the "target" field of notifications
	cache []*pb.Notification
	lock sync.Mutex
}

func NewNotifer() *Notifer {
	return &Notifer{
		cache: make([]*pb.Notification, 0, 10),
	}
}

func (n *Notifer) SetTarget(t string) {
	n.target = t
}

func (n *Notifer) SetId(id string) {
	n.id = id
}

func (n *Notifer) AddNotification(notification *pb.Notification) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.cache = append(n.cache, notification)
}

// id - block
// eventType - subject
// timeStamp - date
// read - read
func (n *Notifer) AddEvent(id string, eventType string, read bool, timeStamp *timestamp.Timestamp) {
	var date *timestamp.Timestamp
	if (timeStamp == nil) {
		date = ptypes.TimestampNow()
	} else {
		date = timeStamp
	}
	notification := &pb.Notification{
		Date:                 date,
		Subject:              eventType,
		Block:                id,
		Type:                 pb.Notification_RECORD_REPORT,
		Read:                 read,
	}
	n.AddNotification(notification)
}

func (n *Notifer) Flush() {
	for _, notification := range n.cache {
		RecordCh <- notification
	}
}
