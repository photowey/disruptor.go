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

func TestBagSetLookupDelete(t *testing.T) {
	t.Parallel()

	bag := NewBag()

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
