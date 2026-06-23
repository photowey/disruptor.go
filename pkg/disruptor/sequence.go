package disruptor

import "sync/atomic"

const InitialSequenceValue int64 = -1

type Sequence struct {
	value atomic.Int64
}

func NewSequence(initial int64) *Sequence {
	sequence := &Sequence{}
	sequence.Store(initial)

	return sequence
}

func (s *Sequence) Value() int64 {
	return s.value.Load()
}

func (s *Sequence) Store(value int64) {
	s.value.Store(value)
}

func (s *Sequence) Add(delta int64) int64 {
	return s.value.Add(delta)
}

func (s *Sequence) CompareAndSwap(oldValue, newValue int64) bool {
	return s.value.CompareAndSwap(oldValue, newValue)
}
