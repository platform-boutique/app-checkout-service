package obs

import (
	"runtime"

	"github.com/grafana/pyroscope-go"
)

// startProfiling pushes CPU, allocation and in-use profiles to Pyroscope (only called
// when Settings.ProfilingOn). Tags follow OBS-40: service_name, version, environment.
// pyroscope.Start does not wait for the server: an unreachable Pyroscope only logs.
func startProfiling(s Settings) (*pyroscope.Profiler, error) {
	// Mutex and block profiles are empty unless the runtime samples them.
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)
	return pyroscope.Start(pyroscope.Config{
		ApplicationName: s.Service,
		ServerAddress:   s.PyroscopeAddr,
		Logger:          nil, // silent: pyroscope's own log lines are not JSON
		Tags: map[string]string{
			"service_name": s.Service,
			"version":      s.Version,
			"environment":  s.Environment,
		},
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount,
			pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount,
			pyroscope.ProfileBlockDuration,
		},
	})
}
