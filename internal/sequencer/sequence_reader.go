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

type minimumSequenceReader struct {
	sequences []*Sequence
}

func newMinimumSequenceReader(sequences []*Sequence) SequenceReader {
	nonNil := make([]*Sequence, 0, len(sequences))
	for _, sequence := range sequences {
		if sequence == nil {
			continue
		}
		nonNil = append(nonNil, sequence)
	}

	switch len(nonNil) {
	case 0:
		return nil
	case 1:
		return nonNil[0]
	default:
		return minimumSequenceReader{sequences: nonNil}
	}
}

func (r minimumSequenceReader) Value() int64 {
	if len(r.sequences) == 0 {
		return InitialSequenceValue
	}

	minimum := r.sequences[0].Value()
	for _, sequence := range r.sequences[1:] {
		value := sequence.Value()
		if value < minimum {
			minimum = value
		}
	}

	return minimum
}
