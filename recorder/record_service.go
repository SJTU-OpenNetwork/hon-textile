package recorder

import (
	//"bytes"
	"context"
	"fmt"
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
// The workflow of record service is:
//		- The recording peer create a record with a unique key. Keep the creating time stamp.
//		- Waiting for other peers sending their report with the same key back.
//		- Provide function to get the statistics or write it to file.

// Service for sending/receving stream related data - add by Jerry 2020/02/25


// streamServiceProtocol is the current protocol tag
const recordServiceProtocol = protocol.ID("/textile/record/1.0.0")
var log = logging.Logger("record")
//var ErrRedundantReq = fmt.Errorf("Request is redundant")
//var ErrUnknowkStream = fmt.Errorf("Unknown stream")

type RecordService struct {
	service          *service.Service
	online           bool
	sendNotification func(*pb.Notification) error

	// Context for main routine
	ctx context.Context
}

// NewStreamService returns a new stream service
func NewRecordService(
	account *keypair.Full,
	node func() *core.IpfsNode,
	sendNotification func(*pb.Notification) error,
	ctx context.Context,
) *RecordService {
	handler := &RecordService{
		sendNotification: sendNotification,
		ctx:			  ctx,
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
}

// Ping pings another peer
func (h *RecordService) Ping(pid peer.ID) (service.PeerStatus, error) {
	return h.service.Ping(pid.Pretty())
}

// Handle is called by the underlying service handler method
func (h *RecordService) Handle(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	//fmt.Printf("core/record_service.go Handler: New message receive from %s.\n", pid.Pretty())
	switch env.Message.Type {
	case pb.RECORD_REPORT:
		return h.handleRecordReport(env, pid)
	default:
		fmt.Printf("core/stream_service.go Handler: Unknown message type %s\n", env.Message.Type.String())
		return nil, nil
	}
}

func (h *RecordService) handleRecordReport(env *pb.Envelope, pid peer.ID) (*pb.Envelope, error) {
	log.Debugf("New record report receive from %s", pid.Pretty())
	report := new(pb.RecordReport)
	err := ptypes.UnmarshalAny(env.Message.Payload, report)
	if err != nil {
		return nil, err
	}

	// Save the report to record
	return nil, err
}

// HandleStream is called by the underlying service handler method
func (h *RecordService) HandleStream(env *pb.Envelope, pid peer.ID) (chan *pb.Envelope, chan error, chan interface{}) {
	return make(chan *pb.Envelope), make(chan error), make(chan interface{})
}

// SendMessage sends a message to a peer.
func (h *RecordService) SendMessage(ctx context.Context, peerId string, env *pb.Envelope) error {
	return h.service.SendMessage(ctx, peerId, env)
}



