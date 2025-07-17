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
	if _, ok := pq.Peek(); ok {
		t.Error("expected queue to be empty after pop")
	}
}

func TestPriorityQueue_Peek(t *testing.T) {
	pq := NewPriorityQueue[int](false)
	if v, ok := pq.Peek(); ok || v != 0 {
		t.Error("peek on empty queue should return zero value and false")
	}
	pq.Push(5, 2.0)
	pq.Push(6, 1.0)
	if v, ok := pq.Peek(); !ok || v != 6 {
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
