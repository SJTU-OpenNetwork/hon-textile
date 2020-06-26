package util

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

type testItem struct {
	value int
}

func Execute(m *SyncMap) {
	r := rand.Intn(10)
	rstr := fmt.Sprintf("%dms", r)
	d, _ := time.ParseDuration(rstr)
	m.Push(rstr, &testItem{value: r})
	fmt.Printf("Add %d\n", r)
	time.Sleep(d)
	re := m.Get(rstr)
	if re != nil {
		fmt.Printf("Get %d\n", re.(*testItem).value)
		m.Pop(rstr)
	}

}

func TestSyncMap(t *testing.T) {
	m := NewSyncMap()
	for i:=0; i<20; i++ {
		go Execute(m)
	}
}
