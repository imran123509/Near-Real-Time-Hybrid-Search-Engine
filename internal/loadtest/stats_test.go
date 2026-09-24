package loadtest

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func durations(ms ...int) []time.Duration {
	out := make([]time.Duration, 0, len(ms))
	for _, n := range ms {
		out = append(out, time.Duration(n)*time.Millisecond)
	}
	return out
}

// The percentiles are what every report is read from, so they are pinned to
// worked examples rather than to whatever the implementation happens to do.
func TestSummarizePercentiles(t *testing.T) {
	// 1..100ms: with nearest-rank, the p-th percentile of 100 sorted samples
	// is simply the p-th of them.
	samples := make([]time.Duration, 0, 100)
	for n := 1; n <= 100; n++ {
		samples = append(samples, time.Duration(n)*time.Millisecond)
	}
	// Out of order on purpose: Summarize must not depend on the input order.
	samples[0], samples[99] = samples[99], samples[0]

	got := Summarize(samples)
	want := Summary{
		Count: 100,
		Min:   1 * time.Millisecond,
		Max:   100 * time.Millisecond,
		Mean:  50*time.Millisecond + 500*time.Microsecond,
		P50:   50 * time.Millisecond,
		P90:   90 * time.Millisecond,
		P95:   95 * time.Millisecond,
		P99:   99 * time.Millisecond,
	}
	if got != want {
		t.Errorf("Summarize = %+v, want %+v", got, want)
	}
}

func TestSummarizeSmallSamples(t *testing.T) {
	tests := []struct {
		name    string
		samples []time.Duration
		want    Summary
	}{
		{"no samples", nil, Summary{}},
		{
			name:    "one sample",
			samples: durations(7),
			want: Summary{
				Count: 1, Min: 7 * time.Millisecond, Max: 7 * time.Millisecond, Mean: 7 * time.Millisecond,
				P50: 7 * time.Millisecond, P90: 7 * time.Millisecond, P95: 7 * time.Millisecond, P99: 7 * time.Millisecond,
			},
		},
		{
			// The tail of a small sample is the worst value seen, not an
			// interpolation past it.
			name:    "two samples",
			samples: durations(10, 20),
			want: Summary{
				Count: 2, Min: 10 * time.Millisecond, Max: 20 * time.Millisecond, Mean: 15 * time.Millisecond,
				P50: 10 * time.Millisecond, P90: 20 * time.Millisecond, P95: 20 * time.Millisecond, P99: 20 * time.Millisecond,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Summarize(tt.samples); got != tt.want {
				t.Errorf("Summarize = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSummarizeDoesNotModifyItsInput(t *testing.T) {
	samples := durations(30, 10, 20)
	Summarize(samples)
	if samples[0] != 30*time.Millisecond {
		t.Errorf("Summarize sorted the caller's slice: %v", samples)
	}
}

func TestRecorderIsSafeForConcurrentUse(t *testing.T) {
	var (
		recorder Recorder
		wg       sync.WaitGroup
	)
	for n := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recorder.Record(time.Duration(n) * time.Millisecond)
		}()
	}
	wg.Wait()

	if got := recorder.Len(); got != 50 {
		t.Fatalf("recorded %d samples, want 50", got)
	}
	if got := recorder.Summary().Count; got != 50 {
		t.Errorf("Summary counted %d samples, want 50", got)
	}
}

// Several runs of one scenario are reported as their median: the fastest run
// is not what the system does.
func TestMedian(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{"nothing", nil, 0},
		{"one run", []float64{207.5}, 207.5},
		{"three runs", []float64{190, 210, 200}, 200},
		{"four runs", []float64{100, 200, 300, 400}, 250},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Median(tt.values); got != tt.want {
				t.Errorf("Median(%v) = %v, want %v", tt.values, got, tt.want)
			}
		})
	}
}

func TestRate(t *testing.T) {
	if got := Rate(500, 2*time.Second); got != 250 {
		t.Errorf("Rate = %v, want 250", got)
	}
	// A rate needs time to have passed; anything else would be an infinity in
	// a report.
	for _, tt := range []struct {
		count   int
		elapsed time.Duration
	}{{100, 0}, {0, time.Second}, {100, -time.Second}} {
		if got := Rate(tt.count, tt.elapsed); got != 0 {
			t.Errorf("Rate(%d, %s) = %v, want 0", tt.count, tt.elapsed, got)
		}
	}
}

// A run that did not index everything must say so; reporting only what
// arrived would turn a failure into a good-looking number.
func TestIndexingResultReportsWhatIsMissing(t *testing.T) {
	result := IndexingResult{
		RunID:          "bench-1",
		Produced:       1000,
		Indexed:        998,
		InsertDuration: 2 * time.Second,
		Duration:       10 * time.Second,
		Latency:        Summarize(durations(100, 200, 300, 400)),
		Documents:      998,
		Points:         998,
	}

	if result.Missing() != 2 {
		t.Errorf("Missing = %d, want 2", result.Missing())
	}
	if got := result.Throughput(); got != 99.8 {
		t.Errorf("Throughput = %v, want 99.8", got)
	}
	if got := result.InsertRate(); got != 500 {
		t.Errorf("InsertRate = %v, want 500", got)
	}

	report := result.String()
	for _, want := range []string{"Produced:", "1000", "Indexed:", "998", "Missing:", "99.8 events/sec", "p95"} {
		if !strings.Contains(report, want) {
			t.Errorf("the report does not mention %q:\n%s", want, report)
		}
	}
}

func TestSummaryString(t *testing.T) {
	if got := (Summary{}).String(); got != "no samples" {
		t.Errorf("empty summary reads %q", got)
	}
	got := Summarize(durations(10, 20, 30)).String()
	for _, want := range []string{"n=3", "p50=", "p99=", "ms"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q does not contain %q", got, want)
		}
	}
}
