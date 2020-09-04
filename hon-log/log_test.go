package honlog

import (
	"fmt"
	"testing"
)

func TestLog(t *testing.T) {
	tSize := 20
	logger := NewLoggerWithSize(tSize)
	t.Log("Write 10000 logs")
	for i:=0; i<10000; i++{
		logger.Add(fmt.Sprintf("Log: %d", i))
	}
	outChan := make(chan string)
	go func(){
		for {
			s, ok:= <- outChan
			if ok {
				fmt.Printf("Get: %s\n", s)
			} else {
				//fmt.Printf("Get: %s\n", s)
				t.Log("Channel close")
				return
			}
		}
	}()

	logger.OutputChan(outChan)

	testcb := func(s string) {
		fmt.Printf("Func Get: %s\n", s)
	}

	testdone := func() {
		fmt.Println("Func End")
	}

	logger.OutputFunc(testcb, testdone)
}

func TestTree(t *testing.T) {
	tree := NewTree("A", nil)
	err := tree.Append("B", nil, "C")
	if err != nil {
		t.Error(err)
	}
	err = tree.Append("E", nil, "D")
	if err != nil {
		t.Error(err)
	}
	err = tree.Append("C", nil, "A")
	if err != nil {
		t.Error(err)
	}
	err = tree.Append("F", nil, "D")
	if err != nil {
		t.Error(err)
	}
	err = tree.Append("D", nil, "A")
	if err != nil {
		t.Error(err)
	}
	err = tree.Append("G", nil, "D")
	if err != nil {
		t.Error(err)
	}
	err = tree.Append("H", nil, "A")
	if err != nil {
		t.Error(err)
	}
	err = tree.Append("I", nil, "H")
	if err != nil {
		t.Error(err)
	}
	err = tree.WriteCSV("test.csv")
	if err != nil {
		t.Error(err)
	}
}
