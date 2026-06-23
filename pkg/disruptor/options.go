package disruptor

type ProducerType uint8

const (
	ProducerTypeUnknown ProducerType = iota
	ProducerTypeSingle
	ProducerTypeMulti
)

type RingBufferOption interface {
	applyRingBuffer(config *ringBufferConfig) error
}

type ringBufferConfig struct {
	producerType ProducerType
	waitStrategy WaitStrategy
	metrics      MetricsSink
}

type ringBufferOptionFunc struct {
	applyFunc ringBufferApplyFunc
}

type ringBufferApplyFunc func(config *ringBufferConfig) error

func (fn ringBufferOptionFunc) applyRingBuffer(config *ringBufferConfig) error {
	return fn.applyFunc(config)
}

func WithProducerType(producerType ProducerType) RingBufferOption {
	return ringBufferOptionFunc{
		applyFunc: func(config *ringBufferConfig) error {
			if producerType == ProducerTypeUnknown {
				return ErrInvalidSequence
			}

			config.producerType = producerType
			return nil
		},
	}
}

func WithWaitStrategy(strategy WaitStrategy) RingBufferOption {
	return ringBufferOptionFunc{
		applyFunc: func(config *ringBufferConfig) error {
			if strategy == nil {
				return ErrInvalidSequence
			}

			config.waitStrategy = strategy
			return nil
		},
	}
}

func WithMetricsSink(sink MetricsSink) RingBufferOption {
	return ringBufferOptionFunc{
		applyFunc: func(config *ringBufferConfig) error {
			if sink == nil {
				config.metrics = nil
				return nil
			}

			config.metrics = sink
			return nil
		},
	}
}

func defaultRingBufferConfig() ringBufferConfig {
	return ringBufferConfig{
		producerType: ProducerTypeMulti,
		waitStrategy: NewBlockingWaitStrategy(),
		metrics:      nil,
	}
}
