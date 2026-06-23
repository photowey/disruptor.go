//go:build !disruptor_cacheline_32 && !disruptor_cacheline_64 && !disruptor_cacheline_128 && !disruptor_cacheline_256 && (arm64 || ppc64 || ppc64le)

package padding

const CacheLineSize = 128
const cacheLineSizeOverridden = false
