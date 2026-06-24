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

package runtimegraph

import "errors"

var (
	// ErrInvalid reports that a runtime graph definition is invalid.
	ErrInvalid = errors.New("runtimegraph: invalid graph")
	// ErrFrozen reports that a runtime graph can no longer be modified.
	ErrFrozen = errors.New("runtimegraph: graph is frozen")
	// ErrHandled reports that a runtime graph has already been registered.
	ErrHandled = errors.New("runtimegraph: graph already handled")
	// ErrNoRoute reports that a runtime graph event selected no terminal route.
	ErrNoRoute = errors.New("runtimegraph: no route")
)
