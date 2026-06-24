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
	"reflect"
	"strings"
	"sync"
)

// RuntimeVariables exposes read-only runtime variable lookup.
type RuntimeVariables interface {
	Lookup(path string) (any, bool)
}

// RuntimeVariablesFunc adapts a function to RuntimeVariables.
type RuntimeVariablesFunc func(path string) (any, bool)

// Lookup calls the wrapped lookup function.
func (fn RuntimeVariablesFunc) Lookup(path string) (any, bool) {
	if fn == nil {
		return nil, false
	}

	return fn(path)
}

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
	mu     sync.RWMutex
	values map[string]any
}

// NewRuntimeBag creates a concurrency-safe event-scoped runtime bag.
func NewRuntimeBag() RuntimeBag {
	return newRuntimeVariableBag()
}

func newRuntimeVariableBag() *runtimeVariableBag {
	return &runtimeVariableBag{
		values: make(map[string]any),
	}
}

func (b *runtimeVariableBag) Lookup(path string) (any, bool) {
	if b == nil {
		return nil, false
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	value, ok := b.values[path]
	return value, ok
}

func (b *runtimeVariableBag) Set(path string, value any) error {
	if err := validateRuntimePath(path); err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("%w: runtime bag is nil", ErrInvalidGraph)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.values[path] = value
	return nil
}

func (b *runtimeVariableBag) Delete(path string) error {
	if err := validateRuntimePath(path); err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("%w: runtime bag is nil", ErrInvalidGraph)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.values, path)
	return nil
}

func (b *runtimeVariableBag) Variables() RuntimeVariables {
	if b == nil {
		return noopRuntimeContext{}
	}

	return b
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
	bag       *runtimeVariableBag
	variables RuntimeVariables
}

func newRuntimeContextWithResolver[T any](
	request EventRequest[T],
	graphName string,
	providerVariables RuntimeVariables,
	resolver EventValueResolver[T],
) *runtimeContext[T] {
	bag := newRuntimeVariableBag()
	return &runtimeContext[T]{
		bag: bag,
		variables: runtimeVariablesView[T]{
			bag:               bag,
			providerVariables: providerVariables,
			resolver:          resolver,
			context:           request.Context,
			event:             request.Event,
			sequence:          request.Sequence,
			graphName:         graphName,
		},
	}
}

func (c *runtimeContext[T]) Lookup(path string) (any, bool) {
	if c == nil || c.bag == nil {
		return nil, false
	}

	return c.bag.Lookup(path)
}

func (c *runtimeContext[T]) Set(path string, value any) error {
	if c == nil || c.bag == nil {
		return fmt.Errorf("%w: runtime context is nil", ErrInvalidGraph)
	}

	return c.bag.Set(path, value)
}

func (c *runtimeContext[T]) Delete(path string) error {
	if c == nil || c.bag == nil {
		return fmt.Errorf("%w: runtime context is nil", ErrInvalidGraph)
	}

	return c.bag.Delete(path)
}

func (c *runtimeContext[T]) Variables() RuntimeVariables {
	if c == nil || c.variables == nil {
		return noopRuntimeContext{}
	}

	return c.variables
}

type runtimeVariablesView[T any] struct {
	bag               RuntimeVariables
	providerVariables RuntimeVariables
	resolver          EventValueResolver[T]
	context           context.Context
	event             *T
	sequence          int64
	graphName         string
}

func (v runtimeVariablesView[T]) Lookup(path string) (any, bool) {
	if v.bag != nil {
		if value, ok := v.bag.Lookup(path); ok {
			return value, true
		}
	}
	if v.providerVariables != nil {
		if value, ok := v.providerVariables.Lookup(path); ok {
			return value, true
		}
	}
	if v.resolver != nil {
		value, ok, err := v.resolver.ResolveEventValue(EventValueResolveRequest[T]{
			Context:   v.context,
			Event:     v.event,
			Sequence:  v.sequence,
			GraphName: v.graphName,
			Path:      path,
		})
		if err == nil && ok {
			return value, true
		}
	}

	return nil, false
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

type reflectionEventValueResolver[T any] struct{}

func newReflectionEventValueResolver[T any]() EventValueResolver[T] {
	return reflectionEventValueResolver[T]{}
}

func (reflectionEventValueResolver[T]) ResolveEventValue(
	request EventValueResolveRequest[T],
) (any, bool, error) {
	if request.Event == nil {
		return nil, false, nil
	}
	if err := validateRuntimePath(request.Path); err != nil {
		return nil, false, err
	}

	return resolveReflectPath(reflect.ValueOf(request.Event), strings.Split(request.Path, "."))
}

func resolveReflectPath(value reflect.Value, parts []string) (any, bool, error) {
	value = indirectReflectValue(value)
	if !value.IsValid() {
		return nil, false, nil
	}
	if len(parts) == 0 {
		return value.Interface(), true, nil
	}

	head := parts[0]
	switch value.Kind() {
	case reflect.Map:
		key := reflect.ValueOf(head)
		if key.Type().AssignableTo(value.Type().Key()) {
			item := value.MapIndex(key)
			if !item.IsValid() {
				return nil, false, nil
			}

			return resolveReflectPath(item, parts[1:])
		}
	case reflect.Struct:
		field := findStructField(value, head)
		if field.IsValid() {
			return resolveReflectPath(field, parts[1:])
		}
	}

	return nil, false, nil
}

func indirectReflectValue(value reflect.Value) reflect.Value {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}

	return value
}

func findStructField(value reflect.Value, name string) reflect.Value {
	valueType := value.Type()
	for i := 0; i < value.NumField(); i++ {
		fieldType := valueType.Field(i)
		if fieldType.PkgPath != "" {
			continue
		}
		if fieldType.Name == name || strings.EqualFold(fieldType.Name, name) {
			return value.Field(i)
		}
		if tagName := strings.Split(fieldType.Tag.Get("json"), ",")[0]; tagName == name {
			return value.Field(i)
		}
	}

	return reflect.Value{}
}

func validateRuntimePath(path string) error {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return fmt.Errorf("%w: runtime path is empty", ErrInvalidGraph)
	}
	if trimmed != path {
		return fmt.Errorf("%w: runtime path %q has surrounding whitespace", ErrInvalidGraph, path)
	}
	for _, part := range strings.Split(path, ".") {
		if part == "" {
			return fmt.Errorf("%w: runtime path %q has an empty segment", ErrInvalidGraph, path)
		}
	}

	return nil
}
