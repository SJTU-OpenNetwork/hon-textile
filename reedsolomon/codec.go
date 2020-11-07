package reedsolomon

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ipfs/go-bitswap/wantlist"
	"github.com/klauspost/reedsolomon"
	"github.com/stretchr/testify/require"

	"unsafe"

)

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
}

func NewShardList(_shardNumber int, _parityNumber int) *ShardList {
	return &ShardList{
		shardNumber:  _shardNumber,
		parityNumber: _parityNumber,
		shardSize:    -1,
		shards:       make([][]byte, _shardNumber + _parityNumber),
	}
}

func (s *ShardList) ShardNumber() int {
	return s.shardNumber
}

func (s *ShardList) ParityNumber() int {
	return s.parityNumber
}

func (s *ShardList) Add(index int, data []byte) error{
	if s.shardSize > 0 && s.shardSize != len(data) {
		return &ErrShardSizeMisMatch{
			want: s.shardSize,
			get:  len(data),
		}
	} else {
		s.shards[index] = data
		s.shardSize = len(data)
	}
	return nil
}

func (s *ShardList) GetData() [][]byte {
	return s.shards
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
	err := c.encoder.Reconstruct(shards)
	if err != nil {
		outError("Error when reconstruct data from shards: ", err)
		return nil, err
	}

	var sizeInfo int
	res := bytesCombine(shards...)
	sizeInfo = Byte2Int(res[:unsafe.Sizeof(sizeInfo)])
	return res[unsafe.Sizeof(sizeInfo): sizeInfo], nil
}

// Split input data into 2-d matrix.
// The size of input data would be put at the first 4 bytes.
func (c *Codec) split(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, reedsolomon.ErrShortData
	}
	sizeInfo := Int2Byte(len(data))
	data = bytesCombine(sizeInfo, data)

	return c.encoder.Split(data)
}

// Combine two byte slices into one
func bytesCombine(pBytes ...[]byte) []byte {
	return bytes.Join(pBytes, []byte(""))
}

func outLog(log string) {
	fmt.Println(log)
}

func outError(prefix string, err error) {
	fmt.Println(prefix, err.Error())
}

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