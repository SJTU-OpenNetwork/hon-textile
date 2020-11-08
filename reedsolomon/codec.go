package reedsolomon

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/recorder"
	logging "github.com/ipfs/go-log"
	"github.com/klauspost/reedsolomon"

	//"unsafe"
)

var log = logging.Logger("reedsolomon")


type ErrShardSizeMisMatch struct {
	want int
	get int
}

func (e *ErrShardSizeMisMatch) Error() string {
	return fmt.Sprintf("shard size mismatch: want %d get %d", e.want, e.get)
}

// ShardList is used to carry data during encoding and decoding.
// The first
type ShardList struct {
	shardNumber int
	parityNumber int
	shardSize int
	shards [][]byte
	size int
}

func NewShardList(_shardNumber int, _parityNumber int) *ShardList {
	return &ShardList{
		shardNumber:  _shardNumber,
		parityNumber: _parityNumber,
		shardSize:    -1,
		shards:       make([][]byte, _shardNumber + _parityNumber),
		size : 0,
	}
}

func (s *ShardList) ShardNumber() int {
	return s.shardNumber
}

func (s *ShardList) ParityNumber() int {
	return s.parityNumber
}

func (s *ShardList) Add(index int, data []byte) error{
	if index > len(s.shards)-1 {
		err := errors.New(fmt.Sprintf("ShardList out of range: %d/%d", index, len(s.shards)))
		outError("Error when add to shardlist: ", err)
		return err
	}

	if s.shardSize > 0 && s.shardSize != len(data) {
		return &ErrShardSizeMisMatch{
			want: s.shardSize,
			get:  len(data),
		}
	} else {
		s.shards[index] = data
		s.shardSize = len(data)
		s.size += 1
	}
	return nil
}

func (s *ShardList) GetData() [][]byte {
	return s.shards
}

func (s *ShardList) Size() int {
	return s.size
}

//func (s *ShardList) GetData()
// A Codec is used to encode bytes to shards
// or decode shards to original bytes.
type Codec struct {
	encoder reedsolomon.Encoder
	shardNumber int
	parityNumber int
}

func NewCodec(_shardNumber int, _parityNumber int) (*Codec, error) {
	c, err := reedsolomon.New(_shardNumber, _parityNumber)
	if err != nil {
		outError("Error when create a new codec: ", err)
		return nil, err
	}
	return &Codec{
		encoder:      c,
		shardNumber:  0,
		parityNumber: 0,
	}, nil
}

// - check size of shard and parity
// - creat a new encoder if necessary.
func (c *Codec) Prepare(_shardNumber int, _parityNumber int) error {
	var err error
	if c.shardNumber == _shardNumber && c.parityNumber == _parityNumber {
		return nil
	} else {
		c.shardNumber = _shardNumber
		c.parityNumber = _parityNumber
		c.encoder, err = reedsolomon.New(_shardNumber, _parityNumber)
		if err != nil {
			outError("Error when create new encoder: ", err)
			return err
		}
		return nil
	}
}

func (c *Codec) EncodeBytes(data []byte) (*ShardList, error) {
	shards, err := c.split(data)
	if err != nil {
		outError("Error when split data into shards: ", err)
		return nil, err
	}

	// Do encode
	err = c.encoder.Encode(shards)
	if err != nil {
		outError("Error when encode shards: ", err)
		return nil, err
	}

	return &ShardList{
		shardNumber:  c.shardNumber,
		parityNumber: c.parityNumber,
		shards:       shards,
	}, nil
}


func (c *Codec) DecodeShardList(shards [][]byte) ([]byte, error) {
	outLog(fmt.Sprintf("decode shardlist with %d shards", len(shards)))
	err := c.encoder.Reconstruct(shards)
	if err != nil {
		outError("Error when reconstruct data from shards: ", err)
		return nil, err
	}

	//var dataLength int64
	res := bytesCombine(shards...)
	//sizeLen := int(unsafe.Sizeof(dataLength))
	//dataLength = BytesToInt64(res[:sizeLen])
	dataLength := Byte2Int(res[:intLength])

	outLog(fmt.Sprintf("decode data size %d, info size %d, total size %d", dataLength, intLength, len(res)))

	// For safety
	if len(res) < dataLength + intLength {
		err = errors.New(fmt.Sprintf("lack data length, need %d, get %d, sizeInfo use %d", dataLength + intLength, len(res), intLength))
		outError("Error when decode shardlist: ", err)
		return nil, err
	}
	outLog("Retrieved: " + string(res[intLength: dataLength + intLength]))
	return res[intLength: dataLength + intLength], nil
}

// Split input data into 2-d matrix.
// The size of input data would be put at the first 4 bytes.
func (c *Codec) split(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, reedsolomon.ErrShortData
	}
	//dataLength := int64(len(data))
	//sizeInfo := Int64ToBytes(dataLength)
	sizeInfo := Int2Byte(len(data))
	data = bytesCombine(sizeInfo, data)
	//fmt.Println(fmt.Sprintf("size info %d bytes, total %d bytes", len(sizeInfo), len(data)))
	return c.encoder.Split(data)
}

// Combine two byte slices into one
func bytesCombine(pBytes ...[]byte) []byte {
	return bytes.Join(pBytes, []byte(""))
}

func outLog(_log string) {
	// fmt.Println(log)
	log.Debug(_log)
	recorder.Hlog.Add(_log)
}

func outError(prefix string, err error) {
	//fmt.Println(prefix, err.Error())
	log.Error(prefix, err)
	recorder.Hlog.Add(prefix + err.Error())
}
/*
// Convert int64 into bytes
func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, unsafe.Sizeof(i))
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

// Convert bytes into int64
func BytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}
*/