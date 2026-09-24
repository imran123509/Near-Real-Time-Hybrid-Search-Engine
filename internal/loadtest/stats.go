package loadtest

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Recorder collects latency samples from several goroutines.
//
// It keeps every sample rather than a running estimate: a benchmark run here
// is thousands of samples, not millions, and exact percentiles are worth more
// than the memory saved by approximating them.
type Recorder struct {
	mu      sync.Mutex
	samples []time.Duration
}

// Record adds one measurement.
func (r *Recorder) Record(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, d)
}

// Len returns how many samples have been recorded.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.samples)
}

// Summary computes the distribution of everything recorded so far.
func (r *Recorder) Summary() Summary {
	r.mu.Lock()
	samples := append([]time.Duration(nil), r.samples...)
	r.mu.Unlock()
	return Summarize(samples)
}

// Summary is a latency distribution.
//
// P50 is what a typical request sees. P95 and P99 are the tail, and they are
// the numbers that matter for a service like this one: a search request fans
// out to OpenSearch, an embedding provider and Qdrant, and waits for all of
// them, so one slow dependency in a hundred calls shows up in nearly every
// request that fans out widely enough. A good average can hide a tail that
// makes the service feel broken.
type Summary struct {
	Count int
	Min   time.Duration
	Max   time.Duration
	Mean  time.Duration
	P50   time.Duration
	P90   time.Duration
	P95   time.Duration
	P99   time.Duration
}

// Summarize returns the distribution of samples. It does not modify samples.
//
// Percentiles use the nearest-rank method: the p-th percentile is the sample
// at ceil(p/100 × n), so every reported value is a measurement that actually
// happened rather than an interpolation between two of them.
func Summarize(samples []time.Duration) Summary {
	if len(samples) == 0 {
		return Summary{}
	}
	sorted := append([]time.Duration(nil), samples...)
	slices.Sort(sorted)

	var total time.Duration
	for _, s := range sorted {
		total += s
	}
	return Summary{
		Count: len(sorted),
		Min:   sorted[0],
		Max:   sorted[len(sorted)-1],
		Mean:  total / time.Duration(len(sorted)),
		P50:   percentile(sorted, 50),
		P90:   percentile(sorted, 90),
		P95:   percentile(sorted, 95),
		P99:   percentile(sorted, 99),
	}
}

// percentile returns the p-th percentile of an already sorted slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	rank = min(max(rank, 1), len(sorted))
	return sorted[rank-1]
}

// String renders the distribution in milliseconds, the unit every report here
// uses.
func (s Summary) String() string {
	if s.Count == 0 {
		return "no samples"
	}
	return fmt.Sprintf("n=%d  min=%s  p50=%s  p90=%s  p95=%s  p99=%s  max=%s  mean=%s",
		s.Count, ms(s.Min), ms(s.P50), ms(s.P90), ms(s.P95), ms(s.P99), ms(s.Max), ms(s.Mean))
}

// ms renders a duration in milliseconds with one decimal, so that reports from
// different runs line up.
func ms(d time.Duration) string {
	return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
}

// Median returns the middle value of a set of run results, which is what
// several runs of the same scenario should be reported as. Taking the fastest
// run would report a best case that no user sees.
func Median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

// Rate returns events per second, and 0 when no time has passed, which is what
// a division would otherwise turn into infinity.
func Rate(count int, elapsed time.Duration) float64 {
	if elapsed <= 0 || count <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}

// IndexingResult is what one indexing benchmark run measured.
//
// Produced and Indexed are deliberately separate: the gap is the part of the
// run that never arrived, and reporting only what arrived would hide it.
type IndexingResult struct {
	RunID string
	// Produced is how many rows were written to PostgreSQL.
	Produced int
	// Indexed is how many of them appeared in the keyword index before the
	// watcher gave up.
	Indexed int
	// InsertDuration is how long writing the rows took, and Duration how long
	// it took from the first insert until the last document was indexed.
	InsertDuration time.Duration
	Duration       time.Duration
	// Latency is the end-to-end time per document, from its insert to the
	// moment it was first seen in the keyword index.
	Latency Summary
	// Documents and Points are how much the two indexes grew during the run.
	// Both equal Produced when nothing was duplicated or lost.
	Documents int
	Points    int
}

// Missing is how many documents never reached the keyword index.
func (r IndexingResult) Missing() int { return r.Produced - r.Indexed }

// Throughput is documents indexed per second across the whole run.
func (r IndexingResult) Throughput() float64 { return Rate(r.Indexed, r.Duration) }

// InsertRate is how fast rows were written to PostgreSQL, which is the rate
// the pipeline was asked to keep up with.
func (r IndexingResult) InsertRate() float64 { return Rate(r.Produced, r.InsertDuration) }

// String renders the result in the format benchmarks/results uses.
func (r IndexingResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Indexing benchmark (run %s)\n\n", r.RunID)
	fmt.Fprintf(&b, "  Produced:        %d rows in %s (%.1f rows/sec into PostgreSQL)\n",
		r.Produced, r.InsertDuration.Round(time.Millisecond), r.InsertRate())
	fmt.Fprintf(&b, "  Indexed:         %d\n", r.Indexed)
	fmt.Fprintf(&b, "  Missing:         %d\n", r.Missing())
	fmt.Fprintf(&b, "  Wall clock:      %s (first insert to last document indexed)\n",
		r.Duration.Round(time.Millisecond))
	fmt.Fprintf(&b, "  Throughput:      %.1f events/sec\n", r.Throughput())
	fmt.Fprintf(&b, "  End-to-end latency per document:\n")
	fmt.Fprintf(&b, "    p50 %s   p90 %s   p95 %s   p99 %s   max %s\n",
		ms(r.Latency.P50), ms(r.Latency.P90), ms(r.Latency.P95), ms(r.Latency.P99), ms(r.Latency.Max))
	fmt.Fprintf(&b, "  Index growth:    %d documents, %d points (both should equal produced)\n",
		r.Documents, r.Points)
	return b.String()
}
