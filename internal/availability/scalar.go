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

package availability

// ScalarScanner scans availability one sequence at a time.
type ScalarScanner struct{}

// NewScalarScanner creates the default scalar availability scanner.
func NewScalarScanner() Scanner {
	return ScalarScanner{}
}

func (s ScalarScanner) HighestPublished(request ScanRequest) int64 {
	if request.Availability == nil || request.AvailableSequence < request.LowerBound {
		return request.LowerBound - 1
	}

	for sequence := request.LowerBound; sequence <= request.AvailableSequence; sequence++ {
		if !request.Availability.Available(sequence) {
			return sequence - 1
		}
	}

	return request.AvailableSequence
}
