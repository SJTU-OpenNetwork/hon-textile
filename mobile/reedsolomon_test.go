package mobile

import (
	"fmt"
	"testing"
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
	reedSolomon := NewReedSolomon(25, 15)

	err := reedSolomon.PrepareCodec(25, 15)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Codec created")
	data := "0123455666aaafgsdfgczvsdfgsaerfgadfgsafghsftghsaregfsdfgsdrtgasdfgsafdgesrgadfgadfgsdfgsdfcvadrgsdfscvsfgaergsafdgva"

	fmt.Println("Test Data:\n" + data)
	fmt.Println(fmt.Sprintf("Data length: %d", len(data)))
	d, err := reedSolomon.NewDecoder(25, 15)

	handler := &TestHandler{}
	reedSolomon.EncodeBytes([]byte(data), 25, 15, handler)
	fmt.Println("Encode Done")

	fmt.Println("Drop data")
	shards[4] = nil
	shards[20] = nil
	shards[12] = nil

	for i := 0; i < 40; i++ {
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
