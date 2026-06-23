package sequencer

import "context"

type Sequencer interface {
	Next(ctx context.Context) (int64, error)
	NextN(ctx context.Context, n int64) (int64, error)
	TryNext() (int64, error)
	TryNextN(n int64) (int64, error)
	Publish(sequence int64)
	PublishRange(lo, hi int64)
	Cursor() *Sequence
	AddGatingSequences(sequences ...*Sequence)
	RemoveGatingSequence(sequence *Sequence) bool
	HighestPublished(lowerBound, availableSequence int64) int64
	Available(sequence int64) bool
}
