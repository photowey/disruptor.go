package disruptor

import "context"

type EventFactory[T any] interface {
	NewEvent() T
}

type EventFactoryFunc[T any] func() T

func (fn EventFactoryFunc[T]) NewEvent() T {
	return fn()
}

type EventTranslator[T any] interface {
	Translate(request TranslateRequest[T])
}

type EventTranslatorFunc[T any] func(request TranslateRequest[T])

func (fn EventTranslatorFunc[T]) Translate(request TranslateRequest[T]) {
	fn(request)
}

type TranslateRequest[T any] struct {
	Context  context.Context
	Event    *T
	Sequence int64
}
