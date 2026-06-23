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

package availability_test

import (
	"fmt"

	"github.com/photowey/disruptor.go/internal/availability"
)

type exampleAvailability map[int64]bool

func (a exampleAvailability) Available(sequence int64) bool {
	return a[sequence]
}

func ExampleScalarScanner() {
	scanner := availability.NewScalarScanner()
	highest := scanner.HighestPublished(availability.ScanRequest{
		LowerBound:        0,
		AvailableSequence: 3,
		Availability: exampleAvailability{
			0: true,
			1: true,
			2: true,
			3: false,
		},
	})
	fmt.Println(highest)

	// Output:
	// 2
}
