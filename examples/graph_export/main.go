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

package main

import (
	"fmt"
	"github.com/photowey/disruptor.go/pkg/event"
	topology "github.com/photowey/disruptor.go/pkg/graph"
)

type exportEvent struct{}

type exportHandler struct{}

func (exportHandler) OnEvent(request event.Request[exportEvent]) error {
	return nil
}

func main() {
	graph := topology.Must[exportEvent]("export").
		MustNode("validate", exportHandler{}).
		MustNode("persist", exportHandler{}).
		MustEdge(topology.StartNode, "validate").
		MustEdge("validate", "persist").
		MustEdge("persist", topology.EndNode)

	snapshot := graph.Snapshot()
	fmt.Printf(
		"graph=%s source=%s entry=%s leaf=%s exit=%s nodes=%d edges=%d\n",
		snapshot.Name,
		snapshot.Sources[0],
		snapshot.Entries[0],
		snapshot.Leaves[0],
		snapshot.Exits[0],
		len(snapshot.Nodes),
		len(snapshot.Edges),
	)
}
