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

import (
	"context"
	"reflect"
	"testing"
)

func TestValidatePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{name: "valid dotted path", path: "route.fast"},
		{name: "empty path", path: "", wantErr: true},
		{name: "trimmed path", path: " route.fast ", wantErr: true},
		{name: "empty segment", path: "route..fast", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidatePath(tt.path)
			if tt.wantErr && err == nil {
				t.Fatalf("ValidatePath(%q) = nil, want error", tt.path)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidatePath(%q) = %v, want nil", tt.path, err)
			}
		})
	}
}

func TestCompilePathSegments(t *testing.T) {
	t.Parallel()

	compiled, err := compilePath("route.fast")
	if err != nil {
		t.Fatalf("compilePath: %v", err)
	}
	if got := compiled.String(); got != "route.fast" {
		t.Fatalf("String = %q, want route.fast", got)
	}
	if got := compiled.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
	if got := compiled.At(0); got != "route" {
		t.Fatalf("At(0) = %q, want route", got)
	}
	if got := compiled.At(1); got != "fast" {
		t.Fatalf("At(1) = %q, want fast", got)
	}
}

func TestCompilePathRejectsInvalidPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "trimmed", path: " route.fast "},
		{name: "empty segment", path: "route..fast"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := compilePath(tt.path); err == nil {
				t.Fatalf("compilePath(%q) = nil, want error", tt.path)
			}
		})
	}
}

func TestBagSetLookupDelete(t *testing.T) {
	t.Parallel()

	bag := NewBag()
	if bag.values != nil {
		t.Fatalf("NewBag allocated values map; want lazy allocation")
	}

	if err := bag.Set("route.fast", true); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, ok := bag.Lookup("route.fast"); !ok || got != true {
		t.Fatalf("Lookup = %v, %v; want true, true", got, ok)
	}
	if err := bag.Delete("route.fast"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := bag.Lookup("route.fast"); ok {
		t.Fatalf("Lookup after delete reported value")
	}
}

func TestBagLookupValueReturnsTypedScalars(t *testing.T) {
	t.Parallel()

	bag := NewBag()
	if err := bag.Set("flags", uint64(3)); err != nil {
		t.Fatalf("Set flags: %v", err)
	}
	if err := bag.Set("enabled", true); err != nil {
		t.Fatalf("Set enabled: %v", err)
	}

	flags, ok, err := bag.LookupValue("flags")
	if err != nil {
		t.Fatalf("LookupValue flags: %v", err)
	}
	if !ok || flags.Kind != TypedValueUint || flags.Uint != 3 {
		t.Fatalf("LookupValue flags = %#v, %v; want uint 3, true", flags, ok)
	}

	enabled, ok, err := bag.LookupValue("enabled")
	if err != nil {
		t.Fatalf("LookupValue enabled: %v", err)
	}
	if !ok || enabled.Kind != TypedValueBool || !enabled.Bool {
		t.Fatalf("LookupValue enabled = %#v, %v; want bool true, true", enabled, ok)
	}
}

func TestBagClearRemovesValuesAndKeepsBackingMap(t *testing.T) {
	t.Parallel()

	bag := NewBag()
	if err := bag.Set("route.fast", true); err != nil {
		t.Fatalf("Set route.fast: %v", err)
	}
	if err := bag.Set("route.audit", false); err != nil {
		t.Fatalf("Set route.audit: %v", err)
	}

	mapPointer := reflect.ValueOf(bag.values).Pointer()
	bag.Clear()

	if got := len(bag.values); got != 0 {
		t.Fatalf("Clear left %d values; want 0", got)
	}
	if _, ok := bag.Lookup("route.fast"); ok {
		t.Fatalf("Lookup after Clear reported value")
	}
	if got := reflect.ValueOf(bag.values).Pointer(); got != mapPointer {
		t.Fatalf("Clear replaced backing map; got %x, want %x", got, mapPointer)
	}
}

func TestTypedValueFromAnyCoversPrimitiveKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  any
		kind TypedValueKind
	}{
		{name: "nil", raw: nil, kind: TypedValueNil},
		{name: "bool", raw: true, kind: TypedValueBool},
		{name: "int", raw: int64(-1), kind: TypedValueInt},
		{name: "uint", raw: uint64(1), kind: TypedValueUint},
		{name: "float", raw: float64(1.5), kind: TypedValueFloat},
		{name: "string", raw: "paid", kind: TypedValueString},
		{name: "object", raw: struct{}{}, kind: TypedValueObject},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := typedValueFromAny(tt.raw); got.Kind != tt.kind {
				t.Fatalf("typedValueFromAny(%T) kind = %v, want %v", tt.raw, got.Kind, tt.kind)
			}
		})
	}
}

func TestViewLookupOrder(t *testing.T) {
	t.Parallel()

	bag := NewBag()
	if err := bag.Set("route.fast", "bag"); err != nil {
		t.Fatalf("Set bag value: %v", err)
	}

	view := View[any]{
		Bag: bag,
		Provider: VariablesFunc(func(path string) (any, bool) {
			if path == "route.fast" {
				return "provider", true
			}
			return nil, false
		}),
		Resolver: ResolverFunc[any](func(_ ResolveRequest[any]) (any, bool, error) {
			return "resolver", true, nil
		}),
	}

	got, ok := view.Lookup("route.fast")
	if !ok || got != "bag" {
		t.Fatalf("Lookup = %v, %v; want bag, true", got, ok)
	}
}

func TestViewLookupValueUsesTypedSources(t *testing.T) {
	t.Parallel()

	view := View[any]{
		Bag: typedVariablesSource{
			values: map[string]TypedValue{
				"route.fast": {Kind: TypedValueBool, Bool: true},
			},
		},
		Provider: VariablesFunc(func(path string) (any, bool) {
			if path == "route.fast" {
				return false, true
			}
			return nil, false
		}),
	}

	got, ok, err := view.LookupValue("route.fast")
	if err != nil {
		t.Fatalf("LookupValue: %v", err)
	}
	if !ok || got.Kind != TypedValueBool || !got.Bool {
		t.Fatalf("LookupValue = %#v, %v; want typed bool true, true", got, ok)
	}
}

func TestViewLookupValueUsesResolverJSONTags(t *testing.T) {
	t.Parallel()

	type event struct {
		RiskScore int64 `json:"risk_score"`
	}

	ev := event{RiskScore: 91}
	view := View[event]{
		Resolver: NewReflectionResolver[event](),
		Event:    &ev,
	}

	got, ok, err := view.LookupValue("risk_score")
	if err != nil {
		t.Fatalf("LookupValue: %v", err)
	}
	if !ok || got.Kind != TypedValueInt || got.Int != 91 {
		t.Fatalf("LookupValue = %#v, %v; want typed int 91, true", got, ok)
	}
}

func TestNewContextExposesMergedVariables(t *testing.T) {
	t.Parallel()

	type event struct {
		Route struct {
			Fast bool `json:"fast"`
		} `json:"route"`
	}

	ev := event{}
	ev.Route.Fast = true

	ctx := NewContext(Request[event]{
		Context:  context.Background(),
		Event:    &ev,
		Sequence: 7,
	}, "graph", nil, NewReflectionResolver[event]())

	if err := ctx.Set("route.audit", false); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if got, ok := ctx.Variables().Lookup("route.audit"); !ok || got != false {
		t.Fatalf("Lookup runtime value = %v, %v; want false, true", got, ok)
	}
	if got, ok := ctx.Variables().Lookup("route.fast"); !ok || got != true {
		t.Fatalf("Lookup event value = %v, %v; want true, true", got, ok)
	}
}

type typedVariablesSource struct {
	values map[string]TypedValue
}

func (s typedVariablesSource) Lookup(path string) (any, bool) {
	value, ok, err := s.LookupValue(path)
	if err != nil || !ok {
		return nil, false
	}

	return value.any(), true
}

func (s typedVariablesSource) LookupValue(path string) (TypedValue, bool, error) {
	value, ok := s.values[path]
	return value, ok, nil
}

func TestReflectionResolverResolveValueReturnsTypedScalars(t *testing.T) {
	t.Parallel()

	type event struct {
		Value int64  `json:"value"`
		Name  string `json:"name"`
	}

	ev := event{
		Value: 42,
		Name:  "paid",
	}
	resolver, ok := NewReflectionResolver[event]().(TypedResolver[event])
	if !ok {
		t.Fatalf("NewReflectionResolver did not implement TypedResolver")
	}

	value, found, err := resolver.ResolveValue(ResolveRequest[event]{
		Context: context.Background(),
		Event:   &ev,
		Path:    "value",
	})
	if err != nil {
		t.Fatalf("ResolveValue value: %v", err)
	}
	if !found || value.Kind != TypedValueInt || value.Int != 42 {
		t.Fatalf("ResolveValue value = %#v, %v; want int 42, true", value, found)
	}

	name, found, err := resolver.ResolveValue(ResolveRequest[event]{
		Context: context.Background(),
		Event:   &ev,
		Path:    "name",
	})
	if err != nil {
		t.Fatalf("ResolveValue name: %v", err)
	}
	if !found || name.Kind != TypedValueString || name.String != "paid" {
		t.Fatalf("ResolveValue name = %#v, %v; want string paid, true", name, found)
	}
}

func TestContextResetClearsBagAndUpdatesRequest(t *testing.T) {
	t.Parallel()

	type event struct {
		Value int64 `json:"value"`
	}

	first := event{Value: 1}
	ctx := NewContext(Request[event]{
		Context:  context.Background(),
		Event:    &first,
		Sequence: 1,
	}, "graph", nil, NewReflectionResolver[event]())
	if err := ctx.Set("route.fast", true); err != nil {
		t.Fatalf("Set: %v", err)
	}

	second := event{Value: 2}
	ctx.Reset(Request[event]{
		Context:  context.Background(),
		Event:    &second,
		Sequence: 2,
	}, "graph", nil, NewReflectionResolver[event]())

	if _, ok := ctx.Lookup("route.fast"); ok {
		t.Fatalf("Lookup after Reset reported stale bag value")
	}
	if got, ok := ctx.Variables().Lookup("value"); !ok || got != int64(2) {
		t.Fatalf("Lookup updated event value = %v, %v; want 2, true", got, ok)
	}
}

func BenchmarkViewLookupValueJSONTagResolver(b *testing.B) {
	type event struct {
		RiskScore int64 `json:"risk_score"`
	}

	ev := event{RiskScore: 91}
	view := View[event]{
		Resolver: NewReflectionResolver[event](),
		Event:    &ev,
	}

	b.ReportAllocs()
	for b.Loop() {
		value, ok, err := view.LookupValue("risk_score")
		if err != nil {
			b.Fatalf("LookupValue: %v", err)
		}
		if !ok || value.Kind != TypedValueInt || value.Int != 91 {
			b.Fatalf("LookupValue = %#v, %v; want typed int 91, true", value, ok)
		}
	}
}
