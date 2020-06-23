package util


// Implementation of container/heap.Interface
// Note:
//	- Push() and Pop() must be function of a pointer instead of the slice itself.
//		That is because the append function may change the address of slice.
//	- Heap is not thread safe. So use some lock to protect it when using in a multi-threads case.
//	- Please make sure that Heap is not empty when call Pop() and Top().
//		Otherwise it would raise an out of index panic.
//		That is caused by heap.Pop() defined in "container.heap".

type HeapItem interface {
	Less(HeapItem) bool
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
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0:n-1]
	return item
}

func (h Heap) Top() interface{} {
	return h[0]
}

func (h Heap) Size() int {
	return len(h)
}

func (h Heap) IsEmpty() bool {
	return len(h)<=0
}

