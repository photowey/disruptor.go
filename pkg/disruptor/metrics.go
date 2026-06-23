package disruptor

import "time"

type MetricsSink interface {
	OnPublish(request PublishMetric)
}

type PublishMetricFunc func(PublishMetric)

type MetricsSinkFunc struct {
	Publish PublishMetricFunc
}

func (f MetricsSinkFunc) OnPublish(request PublishMetric) {
	if f.Publish == nil {
		return
	}

	f.Publish(request)
}

type noopMetricsSink struct{}

func (noopMetricsSink) OnPublish(request PublishMetric) {}

type PublishMetric struct {
	ProducerType ProducerType
	Sequence     int64
	BatchSize    int64
	Duration     time.Duration
	Err          error
}
