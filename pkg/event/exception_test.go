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

package event

import "testing"

type exceptionTestEvent struct{}

func TestFatalExceptionHandlerHaltsEveryFailure(t *testing.T) {
	handler := NewFatalExceptionHandler[exceptionTestEvent]()

	if got := handler.HandleEventException(Exception[exceptionTestEvent]{}); got != ExceptionActionHalt {
		t.Fatalf("event action = %v, want halt", got)
	}
	if got := handler.HandleStartException(LifecycleException{}); got != ExceptionActionHalt {
		t.Fatalf("start action = %v, want halt", got)
	}
	if got := handler.HandleShutdownException(LifecycleException{}); got != ExceptionActionHalt {
		t.Fatalf("shutdown action = %v, want halt", got)
	}
}

func TestIgnoreExceptionHandlerContinuesEveryFailure(t *testing.T) {
	handler := NewIgnoreExceptionHandler[exceptionTestEvent]()

	if got := handler.HandleEventException(Exception[exceptionTestEvent]{}); got != ExceptionActionContinue {
		t.Fatalf("event action = %v, want continue", got)
	}
	if got := handler.HandleStartException(LifecycleException{}); got != ExceptionActionContinue {
		t.Fatalf("start action = %v, want continue", got)
	}
	if got := handler.HandleShutdownException(LifecycleException{}); got != ExceptionActionContinue {
		t.Fatalf("shutdown action = %v, want continue", got)
	}
}

func TestRetryExceptionHandlerTracksSequencesAndReset(t *testing.T) {
	handler, err := NewRetryExceptionHandler[exceptionTestEvent](2, ExceptionActionContinue)
	if err != nil {
		t.Fatalf("new retry handler: %v", err)
	}

	request := Exception[exceptionTestEvent]{Sequence: 7}
	if got := handler.HandleEventException(request); got != ExceptionActionRetry {
		t.Fatalf("first action = %v, want retry", got)
	}
	if got := handler.HandleEventException(request); got != ExceptionActionRetry {
		t.Fatalf("second action = %v, want retry", got)
	}

	handler.ResetRetry(request.Sequence)
	if got := handler.HandleEventException(request); got != ExceptionActionRetry {
		t.Fatalf("after reset action = %v, want retry", got)
	}
	if got := handler.HandleEventException(request); got != ExceptionActionRetry {
		t.Fatalf("after reset second action = %v, want retry", got)
	}
	if got := handler.HandleEventException(request); got != ExceptionActionContinue {
		t.Fatalf("exhausted action = %v, want continue", got)
	}
}

func TestRetryExceptionHandlerRejectsInvalidConfiguration(t *testing.T) {
	if _, err := NewRetryExceptionHandler[exceptionTestEvent](-1, ExceptionActionHalt); err == nil {
		t.Fatal("expected negative retry count to fail")
	}
	if _, err := NewRetryExceptionHandler[exceptionTestEvent](1, ExceptionActionUnknown); err == nil {
		t.Fatal("expected unknown exhausted action to fail")
	}
	if _, err := NewRetryExceptionHandler[exceptionTestEvent](1, ExceptionActionRetry); err == nil {
		t.Fatal("expected retry exhausted action to fail")
	}
}
