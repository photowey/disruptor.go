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

// Package executor provides bounded task execution, typed futures, and promises.
//
// The package is designed around explicit ownership. Executor implementations
// decide where work runs and how backpressure is applied. Future values are
// read-only result handles for callers. Promise values are producer-owned
// completion handles used by task adapters, callbacks, schedulers, or external
// integrations.
//
// Composition helpers such as All, Any, ThenApply, ThenCompose, and
// Exceptionally require an explicit Executor for continuation work. This keeps
// goroutine creation visible to the application and lets callers choose inline,
// fixed worker, or custom execution policies.
package executor
