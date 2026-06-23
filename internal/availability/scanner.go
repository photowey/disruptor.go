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

// Checker reports whether a sequence has been published.
type Checker interface {
	Available(sequence int64) bool
}

// Scanner finds the highest contiguous published sequence.
type Scanner interface {
	HighestPublished(request ScanRequest) int64
}

// ScanRequest carries the range and availability source for a scan.
type ScanRequest struct {
	LowerBound        int64
	AvailableSequence int64
	Availability      Checker
}
