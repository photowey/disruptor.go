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

import "github.com/photowey/disruptor.go/pkg/graph"

// Copy returns a defensive copy of the runtime graph snapshot.
func (s RuntimeGraphSnapshot) Copy() RuntimeGraphSnapshot {
	nodes := make([]graph.NodeSnapshot, len(s.Nodes))
	for i, node := range s.Nodes {
		nodes[i] = graph.NodeSnapshot{
			Name:     node.Name,
			Label:    node.Label,
			Metadata: copyStringMap(node.Metadata),
		}
	}

	edges := append([]RuntimeGraphEdgeSnapshot(nil), s.Edges...)
	sources := append([]string(nil), s.Sources...)
	leaves := append([]string(nil), s.Leaves...)
	entries := append([]string(nil), s.Entries...)
	exits := append([]string(nil), s.Exits...)

	return RuntimeGraphSnapshot{
		Name:    s.Name,
		Frozen:  s.Frozen,
		Nodes:   nodes,
		Edges:   edges,
		Sources: sources,
		Leaves:  leaves,
		Entries: entries,
		Exits:   exits,
	}
}
