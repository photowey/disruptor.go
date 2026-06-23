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

const availableCapacityValue = int64(1<<63 - 1)

type baseSequencerGatingReader struct {
	sequencer *baseSequencer
}

func (r baseSequencerGatingReader) Value() int64 {
	r.sequencer.mu.Lock()
	defer r.sequencer.mu.Unlock()

	if len(r.sequencer.gatingSequences) == 0 {
		return availableCapacityValue
	}

	return r.sequencer.minimumGatingSequenceLocked()
}

type singleProducerGatingReader struct {
	sequencer *singleProducerSequencer
}

func (r singleProducerGatingReader) Value() int64 {
	r.sequencer.gatingMu.RLock()
	defer r.sequencer.gatingMu.RUnlock()

	if len(r.sequencer.gatingSequences) == 0 {
		return availableCapacityValue
	}

	minimum := r.sequencer.gatingSequences[0].Value()
	for _, sequence := range r.sequencer.gatingSequences[1:] {
		value := sequence.Value()
		if value < minimum {
			minimum = value
		}
	}

	return minimum
}
