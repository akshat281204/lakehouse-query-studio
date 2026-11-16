package bench

import (
	"bufio"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// Environment records where a result was measured. Numbers are only
// meaningful together with this block.
type Environment struct {
	GOOS        string `json:"goos"`
	GOARCH      string `json:"goarch"`
	CPUModel    string `json:"cpu_model"`
	LogicalCPUs int    `json:"logical_cpus"`
	MemoryBytes int64  `json:"memory_bytes"`
	GoVersion   string `json:"go_version"`
	DuckDB      string `json:"duckdb_version"`
	Commit      string `json:"git_commit"`
	Dirty       bool   `json:"git_dirty"`
	OSCache     string `json:"os_cache"`
}

func run(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// CaptureEnvironment gathers machine and build details (best effort; unknown
// values are left empty rather than guessed).
func CaptureEnvironment(duckdbVersion string) Environment {
	e := Environment{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(),
		GoVersion: runtime.Version(), DuckDB: duckdbVersion,
		OSCache: "not dropped (needs root); 'first' runs are first-after-generation, not guaranteed cold"}
	switch runtime.GOOS {
	case "darwin":
		e.CPUModel = run("sysctl", "-n", "machdep.cpu.brand_string")
		if n, err := strconv.ParseInt(run("sysctl", "-n", "hw.memsize"), 10, 64); err == nil {
			e.MemoryBytes = n
		}
	case "linux":
		if f, err := os.Open("/proc/cpuinfo"); err == nil {
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				if k, v, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(k) == "model name" {
					e.CPUModel = strings.TrimSpace(v)
					break
				}
			}
			_ = f.Close()
		}
		if f, err := os.Open("/proc/meminfo"); err == nil {
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				if fields := strings.Fields(sc.Text()); len(fields) >= 2 && fields[0] == "MemTotal:" {
					if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
						e.MemoryBytes = kb * 1024
					}
					break
				}
			}
			_ = f.Close()
		}
	}
	e.Commit = run("git", "rev-parse", "HEAD")
	e.Dirty = run("git", "status", "--porcelain") != ""
	return e
}
