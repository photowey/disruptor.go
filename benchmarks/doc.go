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

// Package benchmarks contains release-gate benchmarks for disruptor.go.
//
// The benchmark matrix covers ring sizes, payload shapes, producer/consumer
// topologies, wait strategies, batch sizes, channel comparisons, and latency
// quantiles. The package is intentionally separate from pkg/disruptor so users
// can inspect performance scenarios without mixing benchmark-only helpers into
// the public API package.
package benchmarks
