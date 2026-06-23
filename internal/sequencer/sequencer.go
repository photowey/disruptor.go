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

import "context"

// Sequencer claims producer sequences and tracks publication progress.
type Sequencer interface {
	Next(ctx context.Context) (int64, error)
	NextN(ctx context.Context, n int64) (int64, error)
	TryNext() (int64, error)
	TryNextN(n int64) (int64, error)
	Publish(sequence int64)
	PublishRange(lo, hi int64)
	Cursor() *Sequence
	AddGatingSequences(sequences ...*Sequence)
	RemoveGatingSequence(sequence *Sequence) bool
	HighestPublished(lowerBound, availableSequence int64) int64
	Available(sequence int64) bool
}
