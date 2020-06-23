package util

//import "container/heap"

type HeapItem interface {
	Less(HeapItem) bool
	//String() string
}


type Heap []HeapItem

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
	return h[i].Less(h[j])
}

func (h Heap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *Heap) Push(x interface{}) {
	item := x.(HeapItem)
	*h = append(*h, item)
}

func (h *Heap) Pop() interface{} {
	n := len(*h)
	//if n==0 {
	//	return nil
	//}
	item := (*h)[n-1]
	*h = (*h)[0:n-1]
	return item
}

func (h *Heap) Top() interface{} {
	return (*h)[0]
}

//func ()