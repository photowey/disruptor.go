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

const InitialSequenceValue int64 = -1

type Sequence struct {
	_     padding.CacheLine
	value atomic.Int64
	_     padding.CacheLine
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
