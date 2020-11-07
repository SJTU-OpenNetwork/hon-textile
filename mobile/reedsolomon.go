package mobile

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	"github.com/SJTU-OpenNetwork/hon-textile/reedsolomon"
	"github.com/gogo/protobuf/proto"
)

type ShardList interface {
	ShardNumber() int	// Number of data shards
	ParityNumber() int	// Number of parity shards
	Add(index int, data []byte) error 	// Add a new shard to list
	GetData() [][]byte
}

func (m *Mobile)EncodeBytes(data []byte, shardNumber int, parityNumber int) (ShardList, error){
	var err error
	if m.codec == nil {
		m.codec, err = reedsolomon.NewCodec(shardNumber, parityNumber)
	} else {
		err = m.codec.Prepare(shardNumber, parityNumber)
	}
	if err != nil {
		log.Error("Error when create codec: ", err)
		recorder.Hlog.Add("Error when create codec: " + err.Error())
		return nil, err
	}
	return m.codec.EncodeBytes(data)
}

func (m *Mobile) EncodeBytesToPb(data []byte, shardNumber int, parityNumber int, streamId string, GroupIndex int32, cb ShardPbCallback) {
	list, err := m.EncodeBytes(data, shardNumber, parityNumber)
	if err != nil {
		cb.OnError(err)
		return
	}
	shards := list.GetData()
	for i, shard := range shards {
		shardPb := pb.MulticastData{
			Id:                   streamId,
			Data:                 shard,
			Index:                int32(i),
			GroupIndex:           GroupIndex,
			ShardNum:             int32(shardNumber),
			ParityNum:            int32(parityNumber),
		}
		marshaled, err := proto.Marshal(&shardPb)
		if err != nil {
			cb.OnError(err)
			return
		}
		cb.OnShard(marshaled)
	}
	cb.OnComplete()

}

func (m *Mobile) DecodeShardList(list ShardList) ([]byte, error) {
	var err error
	if m.codec == nil {
		m.codec, err = reedsolomon.NewCodec(list.ShardNumber(), list.ParityNumber())
	} else {
		err = m.codec.Prepare(list.ShardNumber(), list.ParityNumber())
	}
	if err != nil {
		log.Error("Error when create codec: ", err)
		recorder.Hlog.Add("Error when create codec: " + err.Error())
		return nil, err
	}
	return m.codec.DecodeShardList(list.GetData())
}

func (m *Mobile) NewShardList(shardNumber int, parityNumber int) ShardList {
	return reedsolomon.NewShardList(shardNumber, parityNumber)
}
