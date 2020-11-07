package mobile

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	"github.com/SJTU-OpenNetwork/hon-textile/reedsolomon"
	"github.com/gogo/protobuf/proto"
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

func (m *Mobile) getCodec(shardNumber int, parityNumber int) (*reedsolomon.Codec, error) {
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

func (m *Mobile) PrepareCodec(shardNumber int, parityNumber int) error {
	return m.codec.Prepare(shardNumber, parityNumber)
}

func (m *Mobile)encodeBytes(data []byte, shardNumber int, parityNumber int) (*reedsolomon.ShardList, error){
	codec, err := m.getCodec(shardNumber, parityNumber)
	if err != nil {
		log.Error("Error when get codec: ", err)
		recorder.Hlog.Add("Error when get codec: " + err.Error())
		return nil, err
	}
	return codec.EncodeBytes(data)
}

func (m *Mobile) EncodeBytes(data []byte, shardNumber int, parityNumber int, cb ShardCallback) {
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

func (m *Mobile) EncodeBytesToPb(data []byte, shardNumber int, parityNumber int, streamId string, GroupIndex int32, cb ShardCallback) {
	list, err := m.encodeBytes(data, shardNumber, parityNumber)
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

type Decoder interface {
	ShardNumber() int
	ParityNumber() int
	AddData(index int, data []byte) error 	// Add a new shard to list
	AddPb(data []byte) error
	Decode() ([]byte, error)
	Size() int
}

func (m *Mobile) NewDecoder(shardNumber int, parityNumber int) (Decoder, error) {
	codec, err := m.getCodec(shardNumber, parityNumber)
	if err != nil {
		log.Error("Error when get codec: ", err)
		recorder.Hlog.Add("Error when get codec: " + err.Error())
		return nil, err
	}
	return &decoder{
		shardNumber:  shardNumber,
		parityNumber: parityNumber,
		list:         reedsolomon.NewShardList(shardNumber, parityNumber),
		codec:        codec,
	}, nil
}

type decoder struct {
	shardNumber int
	parityNumber int
	list  *reedsolomon.ShardList
	codec *reedsolomon.Codec
}

func (d *decoder) ShardNumber() int {
	return d.shardNumber
}

func (d *decoder) ParityNumber() int {
	return d.parityNumber
}

func (d *decoder) AddData(index int, data []byte) error {
	return d.list.Add(index, data)
}

func (d *decoder) AddPb(data []byte) error {
	shardPb := &pb.MulticastData{
	}
	err := proto.Unmarshal(data, shardPb)
	if err != nil {
		log.Error("Error when unmarshal shard pb: ", err)
		recorder.Hlog.Add("Error when unmarshal shard pb: " + err.Error())
		return err
	}
	return d.list.Add(int(shardPb.Index), shardPb.Data)
}

func (d *decoder) Decode() ([]byte, error) {
	return d.codec.DecodeShardList(d.list.GetData())
}

func (d *decoder) Size() int {
	return d.list.Size()
}