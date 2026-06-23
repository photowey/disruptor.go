// Copyright © 2026-present The Disruptor.go Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sequencer

import (
	"sync/atomic"

	"github.com/photowey/disruptor.go/internal/padding"
)

// InitialSequenceValue is the value before any event has been published.
const InitialSequenceValue int64 = -1

// Sequence is a padded atomic sequence counter.
type Sequence struct {
	_     padding.CacheLine
	value atomic.Int64
	_     padding.CacheLine
}

// NewSequence creates a padded atomic sequence initialized to initial.
func NewSequence(initial int64) *Sequence {
	sequence := &Sequence{}
	sequence.Store(initial)

	return sequence
}

// Value returns the current sequence value.
func (s *Sequence) Value() int64 {
	return s.value.Load()
}

// Store sets the current sequence value.
func (s *Sequence) Store(value int64) {
	s.value.Store(value)
}

// Add atomically adds delta and returns the new sequence value.
func (s *Sequence) Add(delta int64) int64 {
	return s.value.Add(delta)
}

// CompareAndSwap atomically swaps the sequence value when oldValue matches.
func (s *Sequence) CompareAndSwap(oldValue, newValue int64) bool {
	return s.value.CompareAndSwap(oldValue, newValue)
}
