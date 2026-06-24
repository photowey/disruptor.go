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

package graph

import "fmt"

// JoinBuilder expands a group of source nodes into target edges.
type JoinBuilder[T any] interface {
	To(targets ...string) error
	MustTo(targets ...string) *Graph[T]
}

type graphJoinBuilder[T any] struct {
	graph   *Graph[T]
	sources []string
}

// Join starts a source group that can be connected to one or more targets.
func (g *Graph[T]) Join(sources ...string) JoinBuilder[T] {
	return graphJoinBuilder[T]{
		graph:   g,
		sources: append([]string(nil), sources...),
	}
}

func (b graphJoinBuilder[T]) To(targets ...string) error {
	if len(b.sources) == 0 {
		return fmt.Errorf("%w: join sources are empty", ErrInvalid)
	}
	if len(targets) == 0 {
		return fmt.Errorf("%w: join targets are empty", ErrInvalid)
	}

	for _, source := range b.sources {
		for _, target := range targets {
			if err := b.graph.Edge(source, target); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b graphJoinBuilder[T]) MustTo(targets ...string) *Graph[T] {
	if err := b.To(targets...); err != nil {
		panic(err)
	}

	return b.graph
}
