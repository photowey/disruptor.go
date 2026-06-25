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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// RejectPolicy determines fixed worker backpressure behavior.
type RejectPolicy uint8

const (
	// RejectPolicyBlock waits for queue capacity while observing context.
	RejectPolicyBlock RejectPolicy = iota + 1
	// RejectPolicyReject returns ErrSaturated when the queue is full.
	RejectPolicyReject
)

// FixedWorkerOption configures FixedWorkerExecutor.
type FixedWorkerOption interface {
	applyFixedWorker(config *FixedWorkerConfig) error
}

// FixedWorkerConfig is the validated fixed worker configuration.
type FixedWorkerConfig struct {
	Workers      int
	QueueSize    int
	Name         string
	RejectPolicy RejectPolicy
	PanicHandler PanicHandler
	MetricsSink  MetricsSink
}

type fixedWorkerOptionFunc func(config *FixedWorkerConfig) error

func (fn fixedWorkerOptionFunc) applyFixedWorker(config *FixedWorkerConfig) error {
	return fn(config)
}

// WithWorkers sets the fixed worker count.
func WithWorkers(workers int) FixedWorkerOption {
	return fixedWorkerOptionFunc(func(config *FixedWorkerConfig) error {
		if workers < 1 {
			return fmt.Errorf("%w: workers must be positive", ErrInvalid)
		}
		config.Workers = workers
		return nil
	})
}

// WithQueueSize sets the bounded queue size.
func WithQueueSize(size int) FixedWorkerOption {
	return fixedWorkerOptionFunc(func(config *FixedWorkerConfig) error {
		if size < 0 {
			return fmt.Errorf("%w: queue size is negative", ErrInvalid)
		}
		config.QueueSize = size
		return nil
	})
}

// WithName sets the executor name.
func WithName(name string) FixedWorkerOption {
	return fixedWorkerOptionFunc(func(config *FixedWorkerConfig) error {
		name = strings.TrimSpace(name)
		if name == "" {
			return fmt.Errorf("%w: executor name is empty", ErrInvalid)
		}
		config.Name = name
		return nil
	})
}

// WithRejectPolicy sets queue saturation behavior.
func WithRejectPolicy(policy RejectPolicy) FixedWorkerOption {
	return fixedWorkerOptionFunc(func(config *FixedWorkerConfig) error {
		switch policy {
		case RejectPolicyBlock, RejectPolicyReject:
			config.RejectPolicy = policy
			return nil
		default:
			return fmt.Errorf("%w: invalid reject policy", ErrInvalid)
		}
	})
}

// WithPanicHandler sets a recovered panic observer.
func WithPanicHandler(handler PanicHandler) FixedWorkerOption {
	return fixedWorkerOptionFunc(func(config *FixedWorkerConfig) error {
		config.PanicHandler = handler
		return nil
	})
}

// WithMetricsSink sets an executor metrics sink.
func WithMetricsSink(sink MetricsSink) FixedWorkerOption {
	return fixedWorkerOptionFunc(func(config *FixedWorkerConfig) error {
		config.MetricsSink = sink
		return nil
	})
}

// FixedWorkerExecutor runs tasks on a bounded worker pool.
type FixedWorkerExecutor struct {
	config FixedWorkerConfig
	tasks  chan fixedWorkerTask

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	submitMu sync.RWMutex
	closed   atomic.Bool
}

type fixedWorkerTask struct {
	request SubmitRequest
}

type cancelableRunnableTask interface {
	cancelQueued(cause error)
}

// NewFixedWorkerExecutor creates and starts a fixed worker executor.
func NewFixedWorkerExecutor(opts ...FixedWorkerOption) (*FixedWorkerExecutor, error) {
	config := FixedWorkerConfig{
		Workers:      1,
		QueueSize:    0,
		Name:         "executor",
		RejectPolicy: RejectPolicyBlock,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyFixedWorker(&config); err != nil {
			return nil, fmt.Errorf("applying fixed worker option: %w", err)
		}
	}
	if config.Workers < 1 {
		return nil, fmt.Errorf("%w: workers must be positive", ErrInvalid)
	}
	if config.QueueSize < 0 {
		return nil, fmt.Errorf("%w: queue size is negative", ErrInvalid)
	}

	ctx, cancel := context.WithCancel(context.Background())
	executor := &FixedWorkerExecutor{
		config: config,
		tasks:  make(chan fixedWorkerTask, config.QueueSize),
		ctx:    ctx,
		cancel: cancel,
	}
	executor.wg.Add(config.Workers)
	for index := 0; index < config.Workers; index++ {
		go executor.worker()
	}

	return executor, nil
}

// Submit submits a task according to the configured reject policy.
func (e *FixedWorkerExecutor) Submit(request SubmitRequest) error {
	if request.Context == nil {
		request.Context = context.Background()
	}
	if err := request.Context.Err(); err != nil {
		return err
	}
	if request.Task == nil {
		return fmt.Errorf("%w: runnable task is nil", ErrInvalid)
	}
	if e.closed.Load() {
		return ErrClosed
	}

	e.submitMu.RLock()
	defer e.submitMu.RUnlock()
	if e.closed.Load() {
		return ErrClosed
	}

	e.emitMetric("task_submitted", request, nil, 0)
	task := fixedWorkerTask{request: request}
	switch e.config.RejectPolicy {
	case RejectPolicyReject:
		select {
		case e.tasks <- task:
			return nil
		default:
			e.emitMetric("task_rejected", request, ErrSaturated, 0)
			return ErrSaturated
		}
	default:
		select {
		case e.tasks <- task:
			return nil
		case <-request.Context.Done():
			err := request.Context.Err()
			e.emitMetric("task_rejected", request, err, 0)
			return err
		case <-e.ctx.Done():
			return ErrClosed
		}
	}
}

// Shutdown stops accepting tasks and waits for workers to exit.
func (e *FixedWorkerExecutor) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if e.closed.CompareAndSwap(false, true) {
		e.cancel()
		e.submitMu.Lock()
		close(e.tasks)
		e.submitMu.Unlock()
	}

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		e.emitMetric("executor_shutdown", SubmitRequest{}, nil, 0)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *FixedWorkerExecutor) worker() {
	defer e.wg.Done()
	for task := range e.tasks {
		e.runTask(task.request)
	}
}

func (e *FixedWorkerExecutor) runTask(request SubmitRequest) {
	if err := request.Context.Err(); err != nil {
		if task, ok := request.Task.(cancelableRunnableTask); ok {
			task.cancelQueued(err)
		}
		e.emitMetric("task_failed", request, err, 0)
		return
	}

	started := time.Now()
	e.emitMetric("task_started", request, nil, 0)
	defer func() {
		if recovered := recover(); recovered != nil {
			err := fmt.Errorf("executor: task panic: %v", recovered)
			if e.config.PanicHandler != nil {
				e.config.PanicHandler.HandleExecutorPanic(PanicRequest{
					Context:      request.Context,
					ExecutorName: e.config.Name,
					TaskName:     request.Name,
					Recovered:    recovered,
				})
			}
			e.emitMetric("task_panicked", request, err, time.Since(started))
		}
	}()

	request.Task.Run(request.Context)
	e.emitMetric("task_completed", request, nil, time.Since(started))
}

func (e *FixedWorkerExecutor) emitMetric(
	kind string,
	request SubmitRequest,
	err error,
	duration time.Duration,
) {
	if e.config.MetricsSink == nil {
		return
	}

	e.config.MetricsSink.OnExecutorMetric(Metric{
		ExecutorName: e.config.Name,
		TaskName:     request.Name,
		Kind:         kind,
		QueueDepth:   len(e.tasks),
		Workers:      e.config.Workers,
		Duration:     duration,
		Err:          err,
	})
}
