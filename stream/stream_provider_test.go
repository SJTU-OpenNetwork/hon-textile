package stream

import (
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"testing"
)

func TestProvider(t *testing.T) {
	streams := ProvidedStreams{}
	stream1 := "stream1"
	provider1 := "provider1"
	s := streams.getOrCreate(stream1, provider1, 0)

	roots := s.addBlock(&pb.StreamBlock{
		Id:                   "b0",
		Streamid:             stream1,
		Index:                0,
		Size:                 0,
		IsRoot:               false,
		Description:          "",
	})
	t.Log(roots)

	roots = s.addBlock(&pb.StreamBlock{
		Id:                   "b3",
		Streamid:             stream1,
		Index:                3,
		Size:                 0,
		IsRoot:               false,
		Description:          "",
	})
	t.Log(roots)

	roots = s.addBlock(&pb.StreamBlock{
		Id:                   "b2",
		Streamid:             stream1,
		Index:                2,
		Size:                 0,
		IsRoot:               true,
		Description:          "",
	})
	t.Log(roots)

	roots = s.addBlock(&pb.StreamBlock{
		Id:                   "b1",
		Streamid:             stream1,
		Index:                1,
		Size:                 0,
		IsRoot:               false,
		Description:          "",
	})
	t.Log(roots)
}
