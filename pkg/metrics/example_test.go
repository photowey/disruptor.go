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

package metrics_test

import (
	"fmt"

	"github.com/photowey/disruptor.go/pkg/metrics"
)

type countingSink struct {
	published int64
}

func (s *countingSink) OnPublish(request metrics.PublishMetric) {
	s.published += request.BatchSize
}

func (s *countingSink) OnBatchStart(metrics.BatchMetric) {}

func (s *countingSink) OnEventHandled(metrics.EventMetric) {}

func (s *countingSink) OnWait(metrics.WaitMetric) {}

func (s *countingSink) OnProcessorState(metrics.ProcessorMetric) {}

func ExampleSink() {
	sink := &countingSink{}
	sink.OnPublish(metrics.PublishMetric{
		ProducerType: "multi",
		Sequence:     2,
		BatchSize:    3,
	})

	fmt.Println(sink.published)

	// Output:
	// 3
}
