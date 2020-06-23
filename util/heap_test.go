package util

import (
	"container/heap"
	"fmt"
	"testing"
)

type tItem struct {
	v int
}

func (t1 *tItem) Less(t2 HeapItem) bool {
	return t1.v < t2.(*tItem).v
}

func toString(h *Heap) string {
	res := "["
	for _, t := range *h {
		res = fmt.Sprintf("%s, %d", res, t.(*tItem).v)
	}
	return res+"]"
}

func TestHeap(t *testing.T) {
	tHeap := make(Heap, 0, 2)
	//heap.Init(&tHeap)
	heap.Push(&tHeap, &tItem{v:4})
	heap.Push(&tHeap, &tItem{v:1})
	heap.Push(&tHeap, &tItem{v:3})
	heap.Push(&tHeap, &tItem{v:2})
	heap.Push(&tHeap, &tItem{v:0})
	//tHeap = append(tHeap, &tItem{v:1})
	t.Log(toString(&tHeap))
	t.Log(fmt.Sprintf("Top %d", tHeap.Top().(*tItem).v))
	t.Log(fmt.Sprintf("Pop %d", heap.Pop(&tHeap).(*tItem).v))
	t.Log(fmt.Sprintf("Size %d", tHeap.Size()))
	t.Log(toString(&tHeap))
	t.Log(fmt.Sprintf("Top %d", tHeap.Top().(*tItem).v))
	t.Log(fmt.Sprintf("Pop %d", heap.Pop(&tHeap).(*tItem).v))
	t.Log(toString(&tHeap))
	t.Log(fmt.Sprintf("Pop %d", heap.Pop(&tHeap).(*tItem).v))
	t.Log(fmt.Sprintf("Size %d", tHeap.Size()))
	t.Log(fmt.Sprintf("IsEmpty %v", tHeap.IsEmpty()))
	t.Log(fmt.Sprintf("Pop %d", heap.Pop(&tHeap).(*tItem).v))
	t.Log(fmt.Sprintf("IsEmpty %v", tHeap.IsEmpty()))
	t.Log(fmt.Sprintf("Pop %d", heap.Pop(&tHeap).(*tItem).v))
	t.Log(fmt.Sprintf("IsEmpty %v", tHeap.IsEmpty()))
	t.Log(toString(&tHeap))
	//t.Log(heap.Pop(&tHeap))
}
