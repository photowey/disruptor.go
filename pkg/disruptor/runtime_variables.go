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

package disruptor

import (
	"context"
	"fmt"

	runtimevars "github.com/photowey/disruptor.go/internal/runtimevars"
)

// RuntimeVariables exposes read-only runtime variable lookup.
type RuntimeVariables interface {
	Lookup(path string) (any, bool)
}

// RuntimeVariablesFunc adapts a function to RuntimeVariables.
type RuntimeVariablesFunc = runtimevars.VariablesFunc

// RuntimeBag stores event-scoped runtime variables.
type RuntimeBag interface {
	RuntimeVariables
	Set(path string, value any) error
	Delete(path string) error
}

// RuntimeContext exposes runtime variables to runtime graph handlers.
type RuntimeContext interface {
	RuntimeBag
	Variables() RuntimeVariables
}

type runtimeVariableBag struct {
	bag *runtimevars.Bag
}

// NewRuntimeBag creates a concurrency-safe event-scoped runtime bag.
func NewRuntimeBag() RuntimeBag {
	return newRuntimeVariableBag()
}

func newRuntimeVariableBag() *runtimeVariableBag {
	return &runtimeVariableBag{
		bag: runtimevars.NewBag(),
	}
}

func (b *runtimeVariableBag) Lookup(path string) (any, bool) {
	if b == nil || b.bag == nil {
		return nil, false
	}

	return b.bag.Lookup(path)
}

func (b *runtimeVariableBag) Set(path string, value any) error {
	if err := validateRuntimePath(path); err != nil {
		return err
	}
	if b == nil || b.bag == nil {
		return fmt.Errorf("%w: runtime bag is nil", ErrInvalidGraph)
	}

	return wrapRuntimePathError(b.bag.Set(path, value))
}

func (b *runtimeVariableBag) Delete(path string) error {
	if err := validateRuntimePath(path); err != nil {
		return err
	}
	if b == nil || b.bag == nil {
		return fmt.Errorf("%w: runtime bag is nil", ErrInvalidGraph)
	}

	return wrapRuntimePathError(b.bag.Delete(path))
}

func (b *runtimeVariableBag) Variables() RuntimeVariables {
	if b == nil || b.bag == nil {
		return noopRuntimeContext{}
	}

	return b.bag.Variables()
}

// RuntimeVariablesProvider supplies read-only variables for a runtime event.
type RuntimeVariablesProvider[T any] interface {
	Variables(request RuntimeVariablesRequest[T]) (RuntimeVariables, error)
}

// RuntimeVariablesProviderFunc adapts a function to RuntimeVariablesProvider.
type RuntimeVariablesProviderFunc[T any] func(
	request RuntimeVariablesRequest[T],
) (RuntimeVariables, error)

// Variables calls the wrapped provider function.
func (fn RuntimeVariablesProviderFunc[T]) Variables(
	request RuntimeVariablesRequest[T],
) (RuntimeVariables, error) {
	if fn == nil {
		return nil, nil
	}

	return fn(request)
}

// RuntimeVariablesRequest describes a runtime variable provider request.
type RuntimeVariablesRequest[T any] struct {
	Context   context.Context
	Event     *T
	Sequence  int64
	GraphName string
}

// EventValueResolver resolves expression paths from an event value.
type EventValueResolver[T any] interface {
	ResolveEventValue(request EventValueResolveRequest[T]) (any, bool, error)
}

// EventValueResolverFunc adapts a function to EventValueResolver.
type EventValueResolverFunc[T any] func(
	request EventValueResolveRequest[T],
) (any, bool, error)

// ResolveEventValue calls the wrapped resolver function.
func (fn EventValueResolverFunc[T]) ResolveEventValue(
	request EventValueResolveRequest[T],
) (any, bool, error) {
	if fn == nil {
		return nil, false, nil
	}

	return fn(request)
}

// EventValueResolveRequest describes an event field lookup.
type EventValueResolveRequest[T any] struct {
	Context   context.Context
	Event     *T
	Sequence  int64
	GraphName string
	Path      string
}

type runtimeContext[T any] struct {
	inner *runtimevars.Context[T]
}

func newRuntimeContextWithResolver[T any](
	request EventRequest[T],
	graphName string,
	providerVariables RuntimeVariables,
	resolver EventValueResolver[T],
) *runtimeContext[T] {
	return &runtimeContext[T]{
		inner: runtimevars.NewContext(
			runtimevars.Request[T]{
				Context:  request.Context,
				Event:    request.Event,
				Sequence: request.Sequence,
			},
			graphName,
			providerVariables,
			newRuntimeResolverAdapter(resolver),
		),
	}
}

func (c *runtimeContext[T]) Lookup(path string) (any, bool) {
	if c == nil || c.inner == nil {
		return nil, false
	}

	return c.inner.Lookup(path)
}

func (c *runtimeContext[T]) Set(path string, value any) error {
	if c == nil || c.inner == nil {
		return fmt.Errorf("%w: runtime context is nil", ErrInvalidGraph)
	}

	return wrapRuntimePathError(c.inner.Set(path, value))
}

func (c *runtimeContext[T]) Delete(path string) error {
	if c == nil || c.inner == nil {
		return fmt.Errorf("%w: runtime context is nil", ErrInvalidGraph)
	}

	return wrapRuntimePathError(c.inner.Delete(path))
}

func (c *runtimeContext[T]) Variables() RuntimeVariables {
	if c == nil || c.inner == nil {
		return noopRuntimeContext{}
	}

	return c.inner.Variables()
}

type noopRuntimeContext struct{}

func (noopRuntimeContext) Lookup(path string) (any, bool) {
	return nil, false
}

func (noopRuntimeContext) Set(path string, value any) error {
	return validateRuntimePath(path)
}

func (noopRuntimeContext) Delete(path string) error {
	return validateRuntimePath(path)
}

func (c noopRuntimeContext) Variables() RuntimeVariables {
	return c
}

type runtimeResolverAdapter[T any] struct {
	resolver EventValueResolver[T]
}

func newRuntimeResolverAdapter[T any](
	resolver EventValueResolver[T],
) runtimevars.Resolver[T] {
	if resolver == nil {
		return nil
	}

	return runtimeResolverAdapter[T]{resolver: resolver}
}

func (r runtimeResolverAdapter[T]) Resolve(
	request runtimevars.ResolveRequest[T],
) (any, bool, error) {
	if r.resolver == nil {
		return nil, false, nil
	}

	return r.resolver.ResolveEventValue(EventValueResolveRequest[T]{
		Context:   request.Context,
		Event:     request.Event,
		Sequence:  request.Sequence,
		GraphName: request.GraphName,
		Path:      request.Path,
	})
}

type reflectionEventValueResolver[T any] struct {
	inner runtimevars.Resolver[T]
}

func newReflectionEventValueResolver[T any]() EventValueResolver[T] {
	return reflectionEventValueResolver[T]{
		inner: runtimevars.NewReflectionResolver[T](),
	}
}

func (r reflectionEventValueResolver[T]) ResolveEventValue(
	request EventValueResolveRequest[T],
) (any, bool, error) {
	if r.inner == nil {
		return nil, false, nil
	}

	value, ok, err := r.inner.Resolve(runtimevars.ResolveRequest[T]{
		Context:   request.Context,
		Event:     request.Event,
		Sequence:  request.Sequence,
		GraphName: request.GraphName,
		Path:      request.Path,
	})
	if err != nil {
		return nil, false, wrapRuntimePathError(err)
	}

	return value, ok, nil
}

func validateRuntimePath(path string) error {
	return wrapRuntimePathError(runtimevars.ValidatePath(path))
}

func wrapRuntimePathError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%w: %v", ErrInvalidGraph, err)
}
