package reedsolomon

import (
	"fmt"
	"testing"
)

func TestCodec(t *testing.T) {
	codec, err := NewCodec(20, 10)
	if err != nil {
		t.Fatal(err)
	}
	err = codec.Prepare(25,15)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Codec created")
	data := "0123455666aaafgsdfgczvsdfgsaerfgadfgsafghsftghsaregfsdfgsdrtgasdfgsafdgesrgadfgadfgsdfgsdfcvadrgsdfscvsfgaergsafdgva"

	fmt.Println("Test Data:\n"+data)
	fmt.Println(fmt.Sprintf("Data length: %d", len(data)))
	list, err := codec.EncodeBytes([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Encode Done")


	fmt.Println("Drop data")
	list.shards[4] = nil
	list.shards[20] = nil
	list.shards[12] = nil
	rec, err := codec.DecodeShardList(list.shards)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Retrieve: \n" + string(rec))
}
