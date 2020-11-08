package mobile

import (
	"fmt"
	"testing"

	logging "github.com/ipfs/go-log"
)

var shards [][]byte

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

func TestCodec(t *testing.T) {
	logging.SetAllLoggers(logging.LevelDebug)

	reedSolomon := NewReedSolomon(100, 28)

	err := reedSolomon.PrepareCodec(100, 28)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Codec created")
	// data := "0123455666aaafgsdfgczvsdfgsaerfgadfgsafghsftghsaregfsdfgsdrtgasdfgsafdgesrgadfgadfgsdfgsdfcvadrgsdfscvsfgaergsafdgva"
	data := "012345678abcdefghijklmnopqrstuvwxyz"

	fmt.Println("Test Data:\n" + data)
	fmt.Println(fmt.Sprintf("Data length: %d", len(data)))
	d, err := reedSolomon.NewDecoder(100, 28)

	handler := &TestHandler{}
	reedSolomon.EncodeBytes([]byte(data), 100, 28, handler)
	fmt.Println("Encode Done")

	fmt.Println(shards)
	// fmt.Println("Drop data")
	// shards[4] = nil
	// shards[20] = nil
	// shards[12] = nil

	for i := 0; i < 128; i++ {
		if shards[i] != nil {
			d.AddData(i, shards[i])
		}
	}

	fmt.Println("size", d.Size())
	rec, err := d.Decode()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Retrieve: \n" + string(rec))
}
