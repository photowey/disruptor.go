//go:build (disruptor_cacheline_32 && (disruptor_cacheline_64 || disruptor_cacheline_128 || disruptor_cacheline_256)) || (disruptor_cacheline_64 && (disruptor_cacheline_128 || disruptor_cacheline_256)) || (disruptor_cacheline_128 && disruptor_cacheline_256)

package padding

const CacheLineSize = -1
const cacheLineSizeOverridden = true
