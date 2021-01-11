package mobile

import (
	"errors"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	"github.com/SJTU-OpenNetwork/hon-textile/reedsolomon"
	"github.com/golang/protobuf/proto"
	logging "github.com/ipfs/go-log"
)
/*
type ShardList interface {
	ShardNumber() int	// Number of data shards
	ParityNumber() int	// Number of parity shards
	Add(index int, data []byte) error 	// Add a new shard to list
	//GetData() [][]byte
	//traverse() error
}
*/

func (m *ReedSolomon)OpenLog() {
	logging.SetAllLoggers(logging.LevelDebug)
}

type ReedSolomon struct {
	codec *reedsolomon.Codec
}

func NewEmptyReedSolomon() *ReedSolomon{
	return &ReedSolomon{codec: nil}
}

func NewReedSolomon(shardNumber int, parityNumber int) *ReedSolomon {
	codec, err := reedsolomon.NewCodec(shardNumber, parityNumber)
	if err != nil {
		log.Error("Error when create codec: ", err)
		recorder.Hlog.Add("Error when create codec: " + err.Error())
		return &ReedSolomon{codec: nil}
	}
	return &ReedSolomon{codec: codec}
}

func (m *ReedSolomon) getCodec(shardNumber int, parityNumber int) (*reedsolomon.Codec, error) {
	var err error
	var codec *reedsolomon.Codec
	if m.codec == nil {
		codec, err = reedsolomon.NewCodec(shardNumber, parityNumber)
		m.codec = codec
	} else {
		err = m.codec.Prepare(shardNumber, parityNumber)
		codec = m.codec
	}
	if err != nil {
		log.Error("Error when create codec: ", err)
		recorder.Hlog.Add("Error when create codec: " + err.Error())
		return nil, err
	}
	return codec, err
}

func (m *ReedSolomon) PrepareCodec(shardNumber int, parityNumber int) error {
	return m.codec.Prepare(shardNumber, parityNumber)
}

func (m *ReedSolomon)encodeBytes(data []byte, shardNumber int, parityNumber int) (*reedsolomon.ShardList, error){
	codec, err := m.getCodec(shardNumber, parityNumber)
	if err != nil {
		log.Error("Error when get codec: ", err)
		recorder.Hlog.Add("Error when get codec: " + err.Error())
		return nil, err
	}
	return codec.EncodeBytes(data)
}

func (m *ReedSolomon) EncodeBytes(data []byte, shardNumber int, parityNumber int, cb ShardCallback) {
	list, err := m.encodeBytes(data, shardNumber, parityNumber)
	if err != nil {
		cb.OnError(err)
		return
	}
	shards := list.GetData()
	for _, shard := range shards {
		cb.OnShard(shard)
	}
	cb.OnComplete()
}

func (m *ReedSolomon) EncodeBytesToPb(data []byte, shardNumber int, parityNumber int) ([]byte, error) {
	var shardListPb pb.ShardList
	shards := make([]*pb.Shard, shardNumber + parityNumber)
	shardListPb.ShardNumber = int32(shardNumber)
	shardListPb.ParityNumber = int32(parityNumber)
	list, err := m.encodeBytes(data, shardNumber, parityNumber)
	if err != nil {
		recorder.Hlog.Add("Error when encode to pb: " + err.Error())
		log.Error("Error when encode to pb: ", err)
		return nil, err
	}
	for i, s := range list.GetData() {
		shards[i] = &pb.Shard{
			Index:                int32(i),
			Data:                 s,
		}
	}
	shardListPb.Shards = shards
	return proto.Marshal(&shardListPb)
}

func (m *ReedSolomon) DecodePb(data []byte) ([]byte, error) {
	var shardListPb pb.ShardList
	err := proto.Unmarshal(data, &shardListPb)
	if err != nil {
		recorder.Hlog.Add("Error when unmarshal shardlist pb: " + err.Error())
		log.Error("Error when unmarshal shardlist pb: ", err)
		return nil, err
	}

	shards := make([][]byte, shardListPb.ShardNumber + shardListPb.ParityNumber)
	decoder, err := m.getCodec(int(shardListPb.ShardNumber), int(shardListPb.ParityNumber))
	if err != nil {
		recorder.Hlog.Add("Error when get decoder: " + err.Error())
		log.Error("Error when get decoder: ", err)
		return nil, err
	}
	for _, s := range shardListPb.Shards {
		if s.Index > shardListPb.ShardNumber + shardListPb.ParityNumber - 1 {
			return nil, errors.New("shard index out of range")
		}
		shards[s.Index] = s.Data
	}
	return decoder.DecodeShardList(shards)
}