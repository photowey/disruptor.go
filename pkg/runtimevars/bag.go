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

import "sync"

const defaultBagCapacity = 4

// Variables exposes read-only runtime variable lookup.
type Variables interface {
	Lookup(path string) (any, bool)
}

// VariablesFunc adapts a function to Variables.
type VariablesFunc func(path string) (any, bool)

// Lookup calls the wrapped lookup function.
func (fn VariablesFunc) Lookup(path string) (any, bool) {
	if fn == nil {
		return nil, false
	}

	return fn(path)
}

// Bag stores event-scoped runtime variables.
type Bag struct {
	mu     sync.RWMutex
	values map[string]any
}

// NewBag creates a concurrency-safe event-scoped runtime bag.
func NewBag() *Bag {
	return &Bag{}
}

// Lookup returns a value from the bag.
func (b *Bag) Lookup(path string) (any, bool) {
	if b == nil {
		return nil, false
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	value, ok := b.values[path]
	return value, ok
}

// LookupValue returns a typed value from the bag.
func (b *Bag) LookupValue(path string) (TypedValue, bool, error) {
	value, ok := b.Lookup(path)
	if !ok {
		return TypedValue{}, false, nil
	}

	return typedValueFromAny(value), true, nil
}

// Set stores a value in the bag.
func (b *Bag) Set(path string, value any) error {
	if err := ValidatePath(path); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.values == nil {
		b.values = make(map[string]any, defaultBagCapacity)
	}
	b.values[path] = value
	return nil
}

// Delete removes a value from the bag.
func (b *Bag) Delete(path string) error {
	if err := ValidatePath(path); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.values, path)
	return nil
}

// Clear removes every stored value while keeping the backing map available for reuse.
func (b *Bag) Clear() {
	if b == nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for path := range b.values {
		delete(b.values, path)
	}
}

// Variables returns the bag as a read-only variable source.
func (b *Bag) Variables() Variables {
	if b == nil {
		return NoopContext{}
	}

	return b
}
