package primitives

import (
	"testing"
)

func TestPriorityQueue_PushPop(t *testing.T) {
	pq := NewPriorityQueue[int](false)
	pq.Push(1, 3.0)
	pq.Push(2, 1.0)
	pq.Push(3, 2.0)

	expected := []int{2, 3, 1}
	for _, want := range expected {
		got := pq.Pop()
		if got != want {
			t.Errorf("pop: expected %d, got %d", want, got)
		}
	}
}

func TestPriorityQueue_MaxHeap(t *testing.T) {
	pq := NewPriorityQueue[int](true)
	pq.Push(1, 1.0)
	pq.Push(2, 3.0)
	pq.Push(3, 2.0)

	expected := []int{2, 3, 1}
	for _, want := range expected {
		got := pq.Pop()
		if got != want {
			t.Errorf("max heap pop: expected %d, got %d", want, got)
		}
	}
}

func TestPriorityQueue_DuplicateUpdate(t *testing.T) {
	pq := NewPriorityQueue[int](false)
	pq.Push(1, 3.0)
	pq.Push(2, 2.0)
	pq.Push(1, 1.0) // update priority
	if pq.Pop() != 1 {
		t.Error("expected 1 after priority update")
	}
	if pq.Pop() != 2 {
		t.Error("expected 2 after updated item is popped")
	}
	if _, empty := pq.Peek(); !empty {
		t.Error("expected queue to be empty after pop")
	}
}

func TestPriorityQueue_Peek(t *testing.T) {
	pq := NewPriorityQueue[int](false)
	if v, empty := pq.Peek(); !empty || v != 0 {
		t.Error("peek on empty queue should return zero value and true")
	}
	pq.Push(5, 2.0)
	pq.Push(6, 1.0)
	if v, empty := pq.Peek(); empty || v != 6 {
		t.Errorf("peek: expected 6, got %d", v)
	}
}

func TestPriorityQueue_StringType(t *testing.T) {
	pq := NewPriorityQueue[string](false)
	pq.Push("a", 2.0)
	pq.Push("b", 1.0)
	pq.Push("c", 3.0)
	expected := []string{"b", "a", "c"}
	for _, want := range expected {
		got := pq.Pop()
		if got != want {
			t.Errorf("string pop: expected %s, got %s", want, got)
		}
	}
}

func TestPriorityQueue_EmptyPopPanics(t *testing.T) {
	pq := NewPriorityQueue[int](false)
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when popping from empty queue")
		}
	}()
	_ = pq.Pop()
}

func TestPriorityQueue_Remove(t *testing.T) {
	pq := NewPriorityQueue[int](false)

	// add some items
	pq.Push(1, 3.0)
	pq.Push(2, 1.0)
	pq.Push(3, 2.0)
	pq.Push(4, 4.0)

	// test removing an existing item
	if !pq.Remove(2) {
		t.Error("remove should return true for existing item")
	}

	// verify the item is removed and heap property is maintained
	expected := []int{3, 1, 4} // 2 should be gone, others in priority order
	for _, want := range expected {
		got := pq.Pop()
		if got != want {
			t.Errorf("after remove: expected %d, got %d", want, got)
		}
	}
}

func TestPriorityQueue_RemoveNonExistent(t *testing.T) {
	pq := NewPriorityQueue[int](false)
	pq.Push(1, 1.0)
	pq.Push(2, 2.0)

	// test removing non-existent item
	if pq.Remove(99) {
		t.Error("remove should return false for non-existent item")
	}

	// verify queue is unchanged
	if pq.pq.Len() != 2 {
		t.Errorf("queue length should remain 2, got %d", pq.pq.Len())
	}
}

func TestPriorityQueue_RemoveFromEmpty(t *testing.T) {
	pq := NewPriorityQueue[int](false)

	// test removing from empty queue
	if pq.Remove(1) {
		t.Error("remove should return false for empty queue")
	}
}

func TestPriorityQueue_RemoveAndReAdd(t *testing.T) {
	pq := NewPriorityQueue[int](false)

	// add, remove, and re-add the same item
	pq.Push(1, 3.0)
	pq.Push(2, 1.0)

	if !pq.Remove(1) {
		t.Error("should be able to remove item 1")
	}

	// re-add with different priority
	pq.Push(1, 0.5)

	// verify correct order (1 should now have highest priority)
	expected := []int{1, 2}
	for _, want := range expected {
		got := pq.Pop()
		if got != want {
			t.Errorf("after remove and re-add: expected %d, got %d", want, got)
		}
	}
}

func TestPriorityQueue_RemoveMaxHeap(t *testing.T) {
	pq := NewPriorityQueue[int](true) // max heap

	pq.Push(1, 1.0)
	pq.Push(2, 3.0)
	pq.Push(3, 2.0)
	pq.Push(4, 4.0)

	// remove the highest priority item
	if !pq.Remove(4) {
		t.Error("should be able to remove highest priority item")
	}

	// verify remaining items in correct order
	expected := []int{2, 3, 1} // 4 should be gone
	for _, want := range expected {
		got := pq.Pop()
		if got != want {
			t.Errorf("max heap after remove: expected %d, got %d", want, got)
		}
	}
}

func TestPriorityQueue_RemoveStringType(t *testing.T) {
	pq := NewPriorityQueue[string](false)

	pq.Push("a", 3.0)
	pq.Push("b", 1.0)
	pq.Push("c", 2.0)

	// remove middle priority item
	if !pq.Remove("c") {
		t.Error("should be able to remove string item")
	}

	// verify remaining items
	expected := []string{"b", "a"}
	for _, want := range expected {
		got := pq.Pop()
		if got != want {
			t.Errorf("string remove: expected %s, got %s", want, got)
		}
	}
}

func TestPriorityQueue_RemoveAllItems(t *testing.T) {
	pq := NewPriorityQueue[int](false)

	pq.Push(1, 1.0)
	pq.Push(2, 2.0)
	pq.Push(3, 3.0)

	// remove all items
	if !pq.Remove(1) || !pq.Remove(2) || !pq.Remove(3) {
		t.Error("should be able to remove all items")
	}

	// verify queue is empty
	if pq.pq.Len() != 0 {
		t.Errorf("queue should be empty, got length %d", pq.pq.Len())
	}

	if len(pq.idx) != 0 {
		t.Errorf("index map should be empty, got length %d", len(pq.idx))
	}

	// verify peek returns true
	if _, empty := pq.Peek(); !empty {
		t.Error("peek should return true for empty queue")
	}
}
