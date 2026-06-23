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

package disruptor

import (
	"errors"

	sequencer "github.com/photowey/disruptor.go/internal/sequencer"
)

var (
	// ErrAlerted reports that a barrier or processor was alerted.
	ErrAlerted = errors.New("disruptor: alerted")
	// ErrClosed reports that a component has already been started or stopped.
	ErrClosed = errors.New("disruptor: closed")
	// ErrInsufficientCapacity reports that a non-blocking claim cannot proceed.
	ErrInsufficientCapacity = sequencer.ErrInsufficientCapacity
	// ErrInvalidBufferSize reports that a ring buffer size is not a positive power of two.
	ErrInvalidBufferSize = errors.New("disruptor: invalid buffer size")
	// ErrInvalidSequence reports that a sequence request is outside the valid range.
	ErrInvalidSequence = sequencer.ErrInvalidSequence
)
