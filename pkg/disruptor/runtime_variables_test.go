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

package disruptor_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func TestRuntimeBagSetLookupDelete(t *testing.T) {
	bag := disruptor.NewRuntimeBag()

	if err := bag.Set("risk.score", int64(98)); err != nil {
		t.Fatalf("set risk score: %v", err)
	}
	if got, ok := bag.Lookup("risk.score"); !ok || got != int64(98) {
		t.Fatalf("risk.score = %v, %v; want 98, true", got, ok)
	}

	if err := bag.Set("risk.score", int64(42)); err != nil {
		t.Fatalf("overwrite risk score: %v", err)
	}
	if got, ok := bag.Lookup("risk.score"); !ok || got != int64(42) {
		t.Fatalf("risk.score after overwrite = %v, %v; want 42, true", got, ok)
	}

	if err := bag.Delete("risk.score"); err != nil {
		t.Fatalf("delete risk score: %v", err)
	}
	if got, ok := bag.Lookup("risk.score"); ok {
		t.Fatalf("risk.score after delete = %v, true; want missing", got)
	}
	if err := bag.Set("risk..score", 1); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("invalid path error = %v, want ErrInvalidGraph", err)
	}
}

func TestRuntimeBagConcurrentAccess(t *testing.T) {
	bag := disruptor.NewRuntimeBag()

	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(value int) {
			defer wg.Done()
			if err := bag.Set("counter", value); err != nil {
				t.Errorf("set counter: %v", err)
			}
			_, _ = bag.Lookup("counter")
		}(i)
	}
	wg.Wait()

	if _, ok := bag.Lookup("counter"); !ok {
		t.Fatal("counter missing after concurrent writes")
	}
}
