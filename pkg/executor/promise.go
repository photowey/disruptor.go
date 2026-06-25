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

// Promise owns completion of a Future.
type Promise[T any] interface {
	Future() Future[T]
	Complete(value T) bool
	Fail(err error) bool
	Cancel(cause error) bool
}

type promise[T any] struct {
	state *futureState[T]
}

// NewPromise creates an incomplete promise.
func NewPromise[T any]() Promise[T] {
	return &promise[T]{state: newFutureState[T]()}
}

// CompletedFuture creates a future that already completed successfully.
func CompletedFuture[T any](value T) Future[T] {
	promise := NewPromise[T]()
	promise.Complete(value)

	return promise.Future()
}

// FailedFuture creates a future that already completed with err.
func FailedFuture[T any](err error) Future[T] {
	promise := NewPromise[T]()
	promise.Fail(err)

	return promise.Future()
}

// CanceledFuture creates a future that already completed as canceled.
func CanceledFuture[T any](cause error) Future[T] {
	promise := NewPromise[T]()
	promise.Cancel(cause)

	return promise.Future()
}

// Future returns the read-only future view.
func (p *promise[T]) Future() Future[T] {
	return promiseFuture[T]{state: p.state}
}

// Complete completes the promise with a value.
func (p *promise[T]) Complete(value T) bool {
	return p.complete(Result[T]{Value: value})
}

// Fail completes the promise with an error.
func (p *promise[T]) Fail(err error) bool {
	if err == nil {
		err = ErrInvalid
	}

	return p.complete(Result[T]{Err: err})
}

// Cancel completes the promise as canceled.
func (p *promise[T]) Cancel(cause error) bool {
	if cause == nil {
		cause = ErrCanceled
	}

	return p.complete(Result[T]{Err: cause, Canceled: true})
}

func (p *promise[T]) complete(result Result[T]) bool {
	p.state.mu.Lock()
	if p.state.completed {
		p.state.mu.Unlock()
		return false
	}
	p.state.completed = true
	p.state.result = result
	observers := make([]FutureObserver[T], 0, len(p.state.observers))
	for _, observer := range p.state.observers {
		observers = append(observers, observer)
	}
	p.state.observers = nil
	close(p.state.done)
	p.state.mu.Unlock()

	for _, observer := range observers {
		observer.OnFutureComplete(result)
	}

	return true
}
