// Package telemetry exposes bounded-cardinality Prometheus metrics. Every label
// value must come from a closed set declared with the metric: anything else is
// recorded as "other", so a metric can never grow a series per query text,
// object key, dataset id or any other caller-controlled value.
package telemetry

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
)

// Other is the label value used for anything outside a metric's declared set.
const Other = "other"

type family struct {
	name, help, kind string
	labels           []string
	allowed          []map[string]bool
}

func newFamily(name, help, kind string, labels []string, allowed [][]string) family {
	if len(labels) != len(allowed) {
		panic("telemetry: labels and allowed sets differ in length")
	}
	f := family{name: name, help: help, kind: kind, labels: labels}
	for _, vals := range allowed {
		m := map[string]bool{}
		for _, v := range vals {
			m[v] = true
		}
		f.allowed = append(f.allowed, m)
	}
	return f
}

// key validates values against the closed sets and returns the series key.
func (f family) key(values []string) []string {
	if len(values) != len(f.labels) {
		panic("telemetry: wrong number of label values for " + f.name)
	}
	out := make([]string, len(values))
	for i, v := range values {
		if f.allowed[i][v] {
			out[i] = v
		} else {
			out[i] = Other
		}
	}
	return out
}

func (f family) labelStr(values []string, extra ...string) string {
	var parts []string
	for i, l := range f.labels {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, l, values[i]))
	}
	parts = append(parts, extra...)
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (f family) header(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.kind)
}

// Counter is a monotonically increasing labelled counter.
type Counter struct {
	f    family
	mu   sync.Mutex
	vals map[string]float64
	keys map[string][]string
}

// Inc adds 1 to the series for the given label values.
func (c *Counter) Inc(values ...string) { c.Add(1, values...) }

// Add adds n (>= 0) to the series.
func (c *Counter) Add(n float64, values ...string) {
	if n < 0 {
		return
	}
	k := c.f.key(values)
	id := strings.Join(k, "\x00")
	c.mu.Lock()
	c.vals[id] += n
	c.keys[id] = k
	c.mu.Unlock()
}

// Gauge is a labelled value that can go up and down.
type Gauge struct {
	f    family
	mu   sync.Mutex
	vals map[string]float64
	keys map[string][]string
}

// Set sets the series to v.
func (g *Gauge) Set(v float64, values ...string) {
	k := g.f.key(values)
	id := strings.Join(k, "\x00")
	g.mu.Lock()
	g.vals[id] = v
	g.keys[id] = k
	g.mu.Unlock()
}

// Add adds d to the series.
func (g *Gauge) Add(d float64, values ...string) {
	k := g.f.key(values)
	id := strings.Join(k, "\x00")
	g.mu.Lock()
	g.vals[id] += d
	g.keys[id] = k
	g.mu.Unlock()
}

// Histogram observes values into fixed buckets.
type Histogram struct {
	f       family
	buckets []float64
	mu      sync.Mutex
	counts  map[string][]uint64
	sums    map[string]float64
	keys    map[string][]string
}

// Observe records v (seconds, by convention).
func (h *Histogram) Observe(v float64, values ...string) {
	k := h.f.key(values)
	id := strings.Join(k, "\x00")
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.counts[id]
	if !ok {
		c = make([]uint64, len(h.buckets)+1)
		h.counts[id] = c
		h.keys[id] = k
	}
	for i, b := range h.buckets {
		if v <= b {
			c[i]++
		}
	}
	c[len(h.buckets)]++ // +Inf / total count
	h.sums[id] += v
}

// Registry owns a set of metrics and renders them.
type Registry struct {
	mu     sync.Mutex
	order  []string
	render map[string]func(io.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{render: map[string]func(io.Writer){}} }

func (r *Registry) add(name string, fn func(io.Writer)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.render[name]; dup {
		panic("telemetry: duplicate metric " + name)
	}
	r.order = append(r.order, name)
	r.render[name] = fn
}

// NewCounter registers a counter. allowed[i] is the closed value set of labels[i].
func (r *Registry) NewCounter(name, help string, labels []string, allowed [][]string) *Counter {
	c := &Counter{f: newFamily(name, help, "counter", labels, allowed), vals: map[string]float64{}, keys: map[string][]string{}}
	r.add(name, func(w io.Writer) {
		c.f.header(w)
		c.mu.Lock()
		defer c.mu.Unlock()
		ids := sortedKeys(c.vals)
		if len(ids) == 0 && len(labels) == 0 {
			fmt.Fprintf(w, "%s 0\n", name)
		}
		for _, id := range ids {
			fmt.Fprintf(w, "%s%s %s\n", name, c.f.labelStr(c.keys[id]), num(c.vals[id]))
		}
	})
	return c
}

// NewGauge registers a gauge.
func (r *Registry) NewGauge(name, help string, labels []string, allowed [][]string) *Gauge {
	g := &Gauge{f: newFamily(name, help, "gauge", labels, allowed), vals: map[string]float64{}, keys: map[string][]string{}}
	r.add(name, func(w io.Writer) {
		g.f.header(w)
		g.mu.Lock()
		defer g.mu.Unlock()
		ids := sortedKeys(g.vals)
		if len(ids) == 0 && len(labels) == 0 {
			fmt.Fprintf(w, "%s 0\n", name)
		}
		for _, id := range ids {
			fmt.Fprintf(w, "%s%s %s\n", name, g.f.labelStr(g.keys[id]), num(g.vals[id]))
		}
	})
	return g
}

// NewHistogram registers a histogram with the given ascending bucket bounds.
func (r *Registry) NewHistogram(name, help string, buckets []float64, labels []string, allowed [][]string) *Histogram {
	h := &Histogram{f: newFamily(name, help, "histogram", labels, allowed), buckets: buckets,
		counts: map[string][]uint64{}, sums: map[string]float64{}, keys: map[string][]string{}}
	r.add(name, func(w io.Writer) {
		h.f.header(w)
		h.mu.Lock()
		defer h.mu.Unlock()
		ids := make([]string, 0, len(h.counts))
		for id := range h.counts {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			c := h.counts[id]
			for i, b := range h.buckets {
				fmt.Fprintf(w, "%s_bucket%s %d\n", name, h.f.labelStr(h.keys[id], fmt.Sprintf(`le="%s"`, num(b))), c[i])
			}
			fmt.Fprintf(w, "%s_bucket%s %d\n", name, h.f.labelStr(h.keys[id], `le="+Inf"`), c[len(h.buckets)])
			fmt.Fprintf(w, "%s_sum%s %s\n", name, h.f.labelStr(h.keys[id]), num(h.sums[id]))
			fmt.Fprintf(w, "%s_count%s %d\n", name, h.f.labelStr(h.keys[id]), c[len(h.buckets)])
		}
	})
	return h
}

// Render renders all metrics in the Prometheus text exposition format.
func (r *Registry) Render(w io.Writer) {
	r.mu.Lock()
	names := append([]string(nil), r.order...)
	r.mu.Unlock()
	for _, n := range names {
		r.render[n](w)
	}
}

func sortedKeys(m map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func num(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return fmt.Sprintf("%g", v)
}
