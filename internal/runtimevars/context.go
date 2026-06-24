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

package runtimevars

import "context"

// Request describes an event-scoped runtime variable request.
type Request[T any] struct {
	Context  context.Context
	Event    *T
	Sequence int64
}

// Context exposes event-scoped runtime variables to handlers.
type Context[T any] struct {
	bag       *Bag
	variables Variables
}

// NewContext creates a runtime context with bag, provider, and resolver lookup.
func NewContext[T any](
	request Request[T],
	graphName string,
	provider Variables,
	resolver Resolver[T],
) *Context[T] {
	bag := NewBag()
	return &Context[T]{
		bag: bag,
		variables: View[T]{
			Bag:       bag,
			Provider:  provider,
			Resolver:  resolver,
			Context:   request.Context,
			Event:     request.Event,
			Sequence:  request.Sequence,
			GraphName: graphName,
		},
	}
}

// Lookup returns a value from the event-scoped bag.
func (c *Context[T]) Lookup(path string) (any, bool) {
	if c == nil || c.bag == nil {
		return nil, false
	}

	return c.bag.Lookup(path)
}

// Set stores a value in the event-scoped bag.
func (c *Context[T]) Set(path string, value any) error {
	if c == nil || c.bag == nil {
		return ValidatePath(path)
	}

	return c.bag.Set(path, value)
}

// Delete removes a value from the event-scoped bag.
func (c *Context[T]) Delete(path string) error {
	if c == nil || c.bag == nil {
		return ValidatePath(path)
	}

	return c.bag.Delete(path)
}

// Variables returns the merged runtime variable lookup view.
func (c *Context[T]) Variables() Variables {
	if c == nil || c.variables == nil {
		return NoopContext{}
	}

	return c.variables
}

// View merges runtime bag, provider, and event resolver values.
type View[T any] struct {
	Bag       Variables
	Provider  Variables
	Resolver  Resolver[T]
	Context   context.Context
	Event     *T
	Sequence  int64
	GraphName string
}

// Lookup resolves a variable path with bag, provider, then event resolver order.
func (v View[T]) Lookup(path string) (any, bool) {
	if v.Bag != nil {
		if value, ok := v.Bag.Lookup(path); ok {
			return value, true
		}
	}
	if v.Provider != nil {
		if value, ok := v.Provider.Lookup(path); ok {
			return value, true
		}
	}
	if v.Resolver != nil {
		value, ok, err := v.Resolver.Resolve(ResolveRequest[T]{
			Context:   v.Context,
			Event:     v.Event,
			Sequence:  v.Sequence,
			GraphName: v.GraphName,
			Path:      path,
		})
		if err == nil && ok {
			return value, true
		}
	}

	return nil, false
}

// NoopContext implements empty runtime variable lookup and mutation.
type NoopContext struct{}

// Lookup always reports no value.
func (NoopContext) Lookup(path string) (any, bool) {
	return nil, false
}

// Set validates the path and discards the value.
func (NoopContext) Set(path string, value any) error {
	return ValidatePath(path)
}

// Delete validates the path and discards the operation.
func (NoopContext) Delete(path string) error {
	return ValidatePath(path)
}

// Variables returns the no-op context as a read-only variable source.
func (c NoopContext) Variables() Variables {
	return c
}
