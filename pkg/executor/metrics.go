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

package executor

import (
	"context"
	"time"
)

// MetricsSink receives executor metrics.
type MetricsSink interface {
	OnExecutorMetric(metric Metric)
}

// Metric describes an executor event.
type Metric struct {
	ExecutorName string
	TaskName     string
	Kind         string
	QueueDepth   int
	Workers      int
	Duration     time.Duration
	Err          error
}

// PanicHandler receives recovered task panic notifications.
type PanicHandler interface {
	HandleExecutorPanic(request PanicRequest)
}

// PanicHandlerFunc adapts a named function value to PanicHandler.
type PanicHandlerFunc func(request PanicRequest)

// HandleExecutorPanic calls the wrapped function.
func (fn PanicHandlerFunc) HandleExecutorPanic(request PanicRequest) {
	if fn != nil {
		fn(request)
	}
}

// PanicRequest describes a recovered executor panic.
type PanicRequest struct {
	Context      context.Context
	ExecutorName string
	TaskName     string
	Recovered    any
}
