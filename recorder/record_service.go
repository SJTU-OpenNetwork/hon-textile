package recorder

import (
	//"bytes"
	"context"
	"fmt"
	"github.com/segmentio/ksuid"

	//"github.com/SJTU-OpenNetwork/hon-textile/ipfs"
	"github.com/SJTU-OpenNetwork/hon-textile/keypair"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/golang/protobuf/ptypes"
	//"github.com/segmentio/ksuid"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/SJTU-OpenNetwork/hon-textile/service"
	"github.com/SJTU-OpenNetwork/go-ipfs/core"
	peer "github.com/libp2p/go-libp2p-core/peer"
	//"io/ioutil"
	//"time"
)

// recrod service is used to collect statistics.
// For now it is used to analyze the time used for some distributing tasks.
// record service use pb.Notification directly to transform info.
// Fields in notification is used as following:
//		- id string: A unique id used to distinguish different notification.
//			Note that each notification should have its own id.
//		- block string: A unique id used to distinguish different event.
//			It would the cid of file when collecting file distributing information.
//		- actor string: Self peer id.
//			Record may be collected by some other peers. Use this to distinguish the peer generate this record.
//			This may be empty when received from RecordCh. In that case, fill it with self id before sending it.
//		- subject string: Event type of record.
//			A special "final" event is used to represent the final event for an unique id.
//		- date timestamp: Time when this event happens.
//		- target string:  A remote peer id.
//			Record would be sent to that peer if it needs to be collected by other peer.
//			It would be empty if this notification should not be sent to other peer.
//		- read bool:  Whether to send this record to notification channel.
// The workflow of record service is:
//		- Useful information will be sent to RecordCh when some events happen.
//		- Check "read == true ?". Send notification to notification channel if it is.
//		- Check target. Send notification to collector.
//		- Collector receives messages containing notifications from other peers.
//			Send notifications to notification channel if final event received.

// streamServiceProtocol is the current protocol tag
const recordServiceProtocol = protocol.ID("/textile/record/1.0.0")
var log = logging.Logger("record")
var RecordCh = make(chan *pb.Notification, 10)

type RecordService struct {
	service          *service.Service
	online           bool
	sendNotification func(*pb.Notification) error

	recordStore 	 *recordStore
	reportStore		 *reportStore
	peerId 			 string // self peer id.
	// Context for main routine
	ctx context.Context
}

// NewStreamService returns a new stream service
func NewRecordService(
	account *keypair.Full,
	node func() *core.IpfsNode,
	sendNotification func(*pb.Notification) error,
	//peerId string,
	ctx context.Context,
) *RecordService {
	handler := &RecordService{
		//sendNotification: sendNotification,
		ctx:			  ctx,
		recordStore : newRecordStore(),
		reportStore: newReportStore(),
		sendNotification:sendNotification,
		//peerId:node().Identity.Pretty(),	//Should call it after the ipfs node start
	}
	handler.service = service.NewService(account, handler, node)
	return handler
}

// Protocol returns the handler protocol
func (h *RecordService) Protocol() protocol.ID {
	return recordServiceProtocol
}

// Start begins online services
func (h *RecordService) Start() {
	h.online = true
	h.service.Start()
	h.peerId = h.service.Node().Identity.Pretty()
	go h.ListenRecordCh()
}

// Ping pings another peer
func (h *RecordService) Ping(pid peer.ID) (service.PeerStatus, error) {
	return h.service.Ping(pid.Pretty())
}

// Handle is called by the underlying service handler method
func (h *RecordService) Handle(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	//fmt.Printf("core/record_service.go Handler: New message receive from %s.\n", pid.Pretty())
	switch env.Message.Type {
	case pb.Message_RECORD_REPORT:
		return h.handleRecordReport(env, pid)
	case pb.Message_RECORD_NOTIFICATION:
		return h.handleRecordNotification(env, pid)
	default:
		fmt.Printf("core/stream_service.go Handler: Unknown message type %s\n", env.Message.Type.String())
		return nil, nil
	}
}

func (h *RecordService) handleRecordReport(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	log.Debugf("New record report received from %s", pid.Pretty())
	report := new(pb.RecordReport)
	err := ptypes.UnmarshalAny(env.Message.Payload, report)
	if err != nil {
		return nil, err
	}

	// Save the report to record
	return nil, h.recordStore.addReport(report)
}

func (h *RecordService) handleRecordNotification(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	log.Debugf("New record notification received from %s", pid.Pretty())
	notification := new(pb.Notification)
	err := ptypes.UnmarshalAny(env.Message.Payload, notification)
	if err != nil {
		return nil, err
	}
	log.Debugf("Received record info:\n\tblock:\t%s\nsubject:\t%s\nactor:\t%s\ntarget:\t%s",
		notification.Block, notification.Subject, notification.Actor, notification.Target)
	// fill id field before send
	notification.Id = ksuid.New().String()
	err = h.sendNotification(notification)
	if err != nil {
		log.Error("Error occur when send received notification to notification channel.")
		log.Error(err)
		return nil, err
	}
	return nil, nil
}

// HandleStream is called by the underlying service handler method
func (h *RecordService) HandleStream(env *pb.Envelope, pid peer.ID) (chan *pb.Envelope, chan error, chan interface{}) {
	return make(chan *pb.Envelope), make(chan error), make(chan interface{})
}

// SendMessage sends a message to a peer.
func (h *RecordService) sendMessage(ctx context.Context, peerId string, env *pb.Envelope) error {
	return h.service.SendMessage(ctx, peerId, env)
}

// ============ External Interface ============

// get one record in json format
func (h *RecordService) GetRecord(key string) ([]byte, error) {
	return h.recordStore.toJson(key)
}

func (h *RecordService) StartRecord(key string) error {
	return h.recordStore.startRecorder(key)
}

func (h *RecordService) StopRecord(key string) error {
	return h.recordStore.stopRecord(key)
}

func (h *RecordService) StartReport(key string) error {
	err := h.reportStore.add(key)
	if err != nil {
		return err
	}
	return nil
}

func (h *RecordService) StopReport(key string) error {
	err := h.reportStore.stop(key)
	if err != nil {
		return err
	}
	return nil
}

func (h *RecordService) RemoveReport(key string) error {
	return h.reportStore.remove(key)
}

func (h *RecordService) SendReport(key string, peerId string) error {
	// Get report
	report, err := h.reportStore.get(key)
	if err != nil {
		return err
	}

	return h.SendReportPb(report, peerId)
}

func (h *RecordService) SendReportPb(report *pb.RecordReport, peerId string) error {
	env, err := h.service.NewEnvelope(pb.Message_RECORD_REPORT, report, nil, false)
	if err != nil {
		log.Error(err)
		return err
	}
	return h.service.SendMessage(nil, peerId, env)
}

func (h *RecordService) sendNotificationToPeer(notification *pb.Notification, peerId string) error {
	env, err := h.service.NewEnvelope(pb.Message_RECORD_NOTIFICATION, notification, nil, false)
	if err != nil {
		log.Error(err)
		return err
	}
	return h.service.SendMessage(nil, peerId, env)
}

func (h *RecordService) ListenRecordCh() {
	for {
		select {
		case n := <- RecordCh:
			//log.Debugf("Record from channel info:\n\tblock:\t%s\nsubject:\t%s\nactor:\t%s\ntarget:\t%s",
			//	n.Block, n.Subject, n.Actor, n.Target)
			log.Debugf("Record from channel with info:\n%s", n.String())
			err := h.handleRecordChannel(n)
			if err != nil {
				log.Error(err)
			}
		}
	}
}

func (h *RecordService) handleRecordChannel(notification *pb.Notification) error {
	// fill self peer
	notification.Actor = h.peerId

	// fill notification type
	notification.Type = pb.Notification_RECORD_REPORT

	// check whether need to send it to notification channel
	if notification.Read {
		// fill id field before send
		notification.Id = ksuid.New().String()
		err := h.sendNotification(notification)
		if err != nil {return err}
	}

	// check whether need to send it to other peer
	if notification.Target != "" {
		err := h.sendNotificationToPeer(notification, notification.Target)
		if err != nil {
			log.Error("Error occur when send notification from record channel to notification channel.")
			log.Error(err)
			return err
		}
	} else {
		// Do nothing ??
	}
	return nil
}
