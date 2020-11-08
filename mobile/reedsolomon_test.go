package mobile

import (
	"fmt"
	"github.com/SJTU-OpenNetwork/hon-textile/pb"
	"github.com/gogo/protobuf/proto"
	"testing"

	logging "github.com/ipfs/go-log"
)

/*
type TestHandler struct {
}

func (th TestHandler) OnComplete() {
	fmt.Println("OnComplete:")
}

func (th TestHandler) OnShard(data []byte) {
	shards = append(shards, data)
}

func (th TestHandler) OnError(err error) {
	fmt.Println("OnError")
}

 */

func TestCodec(t *testing.T) {
	logging.SetAllLoggers(logging.LevelDebug)

	reedSolomon := NewReedSolomon(100, 28)

	err := reedSolomon.PrepareCodec(100, 28)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Codec created")
	data := "0123455666aaafgsdfgczvsdfgsaerfgadfgsafghsftghsaregfsdfgsdrtgasdfgsafdgesrgadfgadfgsdfgsdfcvadrgsdfscvsfgaergsafdgva"
	//data := "012345678abcdefghijklmnopqrstuvwxyz"

	fmt.Println("Test Data:\n" + data)
	fmt.Println(fmt.Sprintf("Data length: %d", len(data)))
	var listPb pb.ShardList
	listByte, err := reedSolomon.EncodeBytesToPb([]byte(data), 100, 28)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Encode Done")
	err = proto.Unmarshal(listByte, &listPb)
	if err != nil {
		t.Fatal(err)
	}
	//fmt.Println(shards)
	// fmt.Println("Drop data")
	// shards[4] = nil
	// shards[20] = nil
	// shards[12] = nil

	listByteM, err := proto.Marshal(&listPb)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reedSolomon.DecodePb(listByteM)
	if err != nil {
		t.Fatal(err)
	}


	fmt.Println("Retrieve: \n" + string(res))
}
