//go:build !disruptor_cacheline_32 && !disruptor_cacheline_64 && !disruptor_cacheline_128 && !disruptor_cacheline_256 && !(arm || mips || mipsle || mips64 || mips64le || arm64 || ppc64 || ppc64le || s390x)

package padding

const CacheLineSize = 64
const cacheLineSizeOverridden = false
