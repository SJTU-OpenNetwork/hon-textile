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
	tHeap := make(Heap, 0, 10)
	heap.Init(&tHeap)
	heap.Push(&tHeap, &tItem{v:1})
	heap.Push(&tHeap, &tItem{v:3})
	heap.Push(&tHeap, &tItem{v:2})
	heap.Push(&tHeap, &tItem{v:0})
	//tHeap = append(tHeap, &tItem{v:1})
	t.Log(toString(&tHeap))
	t.Log(tHeap.Top())
	t.Log(heap.Pop(&tHeap))
	t.Log(toString(&tHeap))
	t.Log(tHeap.Top())
	t.Log(heap.Pop(&tHeap))
	t.Log(toString(&tHeap))
	t.Log(heap.Pop(&tHeap))
	t.Log(heap.Pop(&tHeap))
	t.Log(toString(&tHeap))
	//t.Log(heap.Pop(&tHeap))
}
