# disruptor.go

[English](README.md) | 中文

`disruptor.go` 是 Go 版本的高性能 Disruptor 模式实现，提供泛型
Ring Buffer、可取消的序列申请、异常恢复、指标钩子、示例和 benchmark。

公共 API 以接口为核心，用户可以无缝替换 factory、translator、handler、
exception handler、wait strategy 和 metrics sink。核心算法可以在内部演进，
而不迫使使用方重写生产者、消费者、指标适配器或恢复策略。

## 安装

```bash
go get github.com/photowey/disruptor.go/pkg/disruptor
```

导入公共包：

```go
import "github.com/photowey/disruptor.go/pkg/disruptor"
```

## 快速开始

```go
package main

import (
    "context"
    "fmt"

    "github.com/photowey/disruptor.go/pkg/disruptor"
)

type LongEvent struct {
    Value int64
}

type LongEventFactory struct{}

func (LongEventFactory) NewEvent() LongEvent {
    return LongEvent{}
}

type LongEventHandler struct {
    Done chan<- int64
}

func (h LongEventHandler) OnEvent(
    request disruptor.EventRequest[LongEvent],
) error {
    h.Done <- request.Event.Value
    return nil
}

type LongEventTranslator struct {
    Value int64
}

func (t LongEventTranslator) Translate(
    request disruptor.TranslateRequest[LongEvent],
) {
    request.Event.Value = t.Value
}

func main() {
    ctx := context.Background()

    d, err := disruptor.New(LongEventFactory{}, 1024)
    if err != nil {
        panic(err)
    }

    done := make(chan int64, 1)
    _, err = d.HandleEventsWith(LongEventHandler{Done: done})
    if err != nil {
        panic(err)
    }
    if err := d.Start(ctx); err != nil {
        panic(err)
    }

    err = d.RingBuffer().PublishEvent(ctx, LongEventTranslator{Value: 42})
    if err != nil {
        panic(err)
    }

    fmt.Println(<-done)

    d.Stop()
    if err := d.Wait(); err != nil {
        panic(err)
    }
}
```

## API 形态

- `RingBuffer[T]` 是底层 API，用于申请、修改和发布预分配事件槽。
- `Disruptor[T]` 是高层门面，围绕一个 Ring Buffer 管理并行消费者。V1 中每个消费者都会收到全部事件。
- `EventFactory[T]`、`EventTranslator[T]`、`EventHandler[T]`、`ExceptionHandler[T]`、`WaitStrategy` 和 `MetricsSink` 都是接口。
- `XxxFunc` 适配器仍然可用，适合快速桥接回调；正式示例优先展示命名类型，避免公开用法过度依赖匿名函数。
- 阻塞生产者和处理器路径都接受 `context.Context`，等待过程可以被取消。
- 默认生产者类型是 `ProducerTypeMulti`；单生产者场景可以使用 `ProducerTypeSingle` 获得更轻的顺序发布路径。

## 项目布局

公共包位于 `pkg/disruptor`。内部算法边界位于 `internal/`：

```text
internal/
  availability/   连续发布扫描
  padding/        按 GOARCH 选择的 cache-line padding 原语
  sequencer/      sequence 原语以及 single/multi producer sequencer

pkg/disruptor/    公共 API、ring buffer facade、barrier、processor、metrics
benchmarks/       端到端和 channel 对比 benchmark
examples/         可运行示例
docs/             API 和设计文档
```

`pkg/disruptor.Sequence` 从 `internal/sequencer` 重新导出，因此外部用户拿到的是稳定公共类型，而内部 sequencing 算法仍可替换。

Cache-line padding 默认按 Go 架构近似选择：32、64、128、256 字节布局会在编译期选择。也可以用构建标签覆盖，用于 benchmark 或特殊目标：
`disruptor_cacheline_32`、`disruptor_cacheline_64`、`disruptor_cacheline_128`、`disruptor_cacheline_256`。

## 异常恢复

默认异常处理器是 fail-fast。你可以替换为 ignore 或有界 retry：

```go
retryHandler, err := disruptor.NewRetryExceptionHandler[LongEvent](
    2,
    disruptor.ExceptionActionHalt,
)
if err != nil {
    panic(err)
}

_, err = d.HandleEventsWithOptions(
    []disruptor.EventHandler[LongEvent]{handler},
    disruptor.WithExceptionHandler[LongEvent](retryHandler),
)
```

handler panic 会被恢复，并进入相同的 exception handler 路径。producer translator panic 会先发布已申请的 sequence，再向调用者重新 panic，避免消费者卡在未发布槽位后面。

## 指标

指标是可选、后端无关的。默认 sink 为 nil，因此热路径会在测量和分发前短路。

```go
type CountingMetricsSink struct{}

func (CountingMetricsSink) OnPublish(metric disruptor.PublishMetric) {}
func (CountingMetricsSink) OnBatchStart(metric disruptor.BatchMetric) {}
func (CountingMetricsSink) OnEventHandled(metric disruptor.EventMetric) {}
func (CountingMetricsSink) OnWait(metric disruptor.WaitMetric) {}
func (CountingMetricsSink) OnProcessorState(metric disruptor.ProcessorMetric) {}
```

## 示例

可运行示例位于 `examples/`：

- `examples/basic`
- `examples/multi_consumer`
- `examples/metrics`
- `examples/error_recovery`
- `examples/batch_publish`
- `examples/single_producer`

运行示例：

```bash
go run ./examples/basic
```

## Benchmark

Benchmark 是发布就绪的一部分：

```bash
go test ./...
go test -race ./...
go test -run '^$' -bench=. -benchmem -benchtime=100ms -count=10 -cpu=1,2,4,8 ./...
go test -run '^$' -bench=BenchmarkE2ELatencyQuantiles -benchmem -count=10 ./benchmarks
benchstat benchmarks/baseline/baseline.txt /tmp/disruptor-new.txt
```

更多端到端、M/N 生产消费、channel、`sync.Cond`、baseline 和尾延迟分组见 `benchmarks/README.md`。

普通所有权转移和简单同步仍然优先使用 channel。只有当 benchmark 证明你需要高吞吐、低分配、广播给多个消费者或可控背压时，再使用这个库。
