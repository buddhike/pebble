package primitives

import (
	"container/heap"
)

type item[T any] struct {
	value    T
	priority float64
	index    int
}

// A pq implements heap.Interface and holds Items.
type pq[T any] struct {
	items    []*item[T]
	reversed bool
}

func (pq *pq[T]) Len() int { return len(pq.items) }

func (pq *pq[T]) Less(i, j int) bool {
	if pq.reversed {
		return pq.items[i].priority > pq.items[j].priority
	}
	return pq.items[i].priority < pq.items[j].priority
}

func (pq *pq[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *pq[T]) Push(x any) {
	n := len(pq.items)
	item := x.(*item[T])
	item.index = n
	pq.items = append(pq.items, item)
}

func (pq *pq[T]) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}

type PriorityQueue[T comparable] struct {
	pq  *pq[T]
	idx map[T]*item[T]
}

func NewPriorityQueue[T comparable](reversed bool) *PriorityQueue[T] {
	return &PriorityQueue[T]{
		pq: &pq[T]{
			items:    make([]*item[T], 0),
			reversed: reversed,
		},
		idx: make(map[T]*item[T]),
	}
}

func (q *PriorityQueue[T]) Push(v T, priority float64) {
	i := q.idx[v]
	if i == nil {
		i = &item[T]{
			value:    v,
			priority: priority,
		}
		q.idx[v] = i
		heap.Push(q.pq, i)
	} else {
		i.priority = priority
		heap.Fix(q.pq, i.index)
	}
}

func (q *PriorityQueue[T]) Pop() T {
	item := heap.Pop(q.pq).(*item[T])
	delete(q.idx, item.value)
	return item.value
}

func (q *PriorityQueue[T]) Peek() (T, bool) {
	items := q.pq.items
	if len(items) == 0 {
		var zero T
		return zero, true
	}
	return items[0].value, false
}

func (q *PriorityQueue[T]) Remove(v T) bool {
	i := q.idx[v]
	if i == nil {
		return false
	}
	delete(q.idx, v)
	heap.Remove(q.pq, i.index)
	return true
}
