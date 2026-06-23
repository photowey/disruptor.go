//go:build !disruptor_cacheline_32 && !disruptor_cacheline_64 && !disruptor_cacheline_128 && !disruptor_cacheline_256 && (arm || mips || mipsle || mips64 || mips64le)

package padding

const CacheLineSize = 32
const cacheLineSizeOverridden = false
