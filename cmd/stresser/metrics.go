package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// sample is a single query result emitted by a worker.
type sample struct {
	dsn     string
	latency time.Duration
	err     error
}

type targetStats struct {
	dsn     string
	queries int
	errors  int
	qps     float64
	p50     time.Duration
	p95     time.Duration
	p99     time.Duration
	max     time.Duration
}

type report struct {
	targets   []targetStats
	aggregate targetStats
}

// buildReport computes per-target and aggregate statistics from collected samples.
// dsns controls the order targets appear in the report.
func buildReport(collected map[string][]sample, dsns []string, measuredDur time.Duration) report {
	r := report{targets: make([]targetStats, 0, len(dsns))}

	var allLatencies []time.Duration

	for _, dsn := range dsns {
		samples := collected[dsn]
		stats := computeStats(dsn, samples, measuredDur)
		r.targets = append(r.targets, stats)
		for _, s := range samples {
			if s.err == nil {
				allLatencies = append(allLatencies, s.latency)
			}
		}
	}

	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })

	agg := targetStats{dsn: "TOTAL"}
	for _, t := range r.targets {
		agg.queries += t.queries
		agg.errors += t.errors
	}
	agg.qps = float64(agg.queries-agg.errors) / measuredDur.Seconds()
	if len(allLatencies) > 0 {
		agg.p50 = percentile(allLatencies, 0.50)
		agg.p95 = percentile(allLatencies, 0.95)
		agg.p99 = percentile(allLatencies, 0.99)
		agg.max = allLatencies[len(allLatencies)-1]
	}
	r.aggregate = agg
	return r
}

func computeStats(dsn string, samples []sample, dur time.Duration) targetStats {
	var latencies []time.Duration
	var errCount int

	for _, s := range samples {
		if s.err != nil {
			errCount++
		} else {
			latencies = append(latencies, s.latency)
		}
	}

	stats := targetStats{
		dsn:     dsn,
		queries: len(latencies) + errCount,
		errors:  errCount,
		qps:     float64(len(latencies)) / dur.Seconds(),
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if len(latencies) > 0 {
		stats.p50 = percentile(latencies, 0.50)
		stats.p95 = percentile(latencies, 0.95)
		stats.p99 = percentile(latencies, 0.99)
		stats.max = latencies[len(latencies)-1]
	}
	return stats
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// ─── Output ──────────────────────────────────────────────────────────────────

func printReport(r report, format, workloadLabel string, workersPerTarget int, duration, warmup time.Duration) {
	switch format {
	case "json":
		printJSON(r, workloadLabel, workersPerTarget)
	case "csv":
		printCSV(r)
	default:
		printTable(r, workloadLabel, workersPerTarget, duration, warmup)
	}
}

// ─── Table ───────────────────────────────────────────────────────────────────

const (
	colTarget = 35
	colN      = 10
	colErr    = 8
	colQPS    = 9
	colLat    = 9
)

func printTable(r report, workloadLabel string, workersPerTarget int, duration, warmup time.Duration) {
	hdr := tableHeader()
	sep := strings.Repeat("─", len(hdr))

	// Meta block
	fmt.Fprintf(os.Stdout, "Workload:  %s\n", workloadLabel)
	if len(r.targets) > 1 {
		fmt.Fprintf(os.Stdout, "Workers:   %d per target (%d total)\n", workersPerTarget, workersPerTarget*len(r.targets))
	} else {
		fmt.Fprintf(os.Stdout, "Workers:   %d\n", workersPerTarget)
	}
	fmt.Fprintf(os.Stdout, "Duration:  %s  (warmup: %s)\n\n", duration, warmup)

	fmt.Fprintln(os.Stdout, hdr)
	fmt.Fprintln(os.Stdout, sep)

	for _, t := range r.targets {
		fmt.Fprintln(os.Stdout, tableRow(t))
	}

	if len(r.targets) > 1 {
		fmt.Fprintln(os.Stdout, sep)
		fmt.Fprintln(os.Stdout, tableRow(r.aggregate))
	}

	fmt.Fprintln(os.Stdout, sep)
}

func tableHeader() string {
	return fmt.Sprintf(
		" %-*s │ %*s │ %*s │ %*s │ %*s │ %*s │ %*s",
		colTarget, "Target",
		colN, "Queries",
		colErr, "Errors",
		colQPS, "QPS",
		colLat, "p50",
		colLat, "p95",
		colLat, "p99",
	)
}

func tableRow(t targetStats) string {
	return fmt.Sprintf(
		" %-*s │ %*d │ %*d │ %*.0f │ %*s │ %*s │ %*s",
		colTarget, truncate(t.dsn, colTarget),
		colN, t.queries,
		colErr, t.errors,
		colQPS, t.qps,
		colLat, fmtLatency(t.p50),
		colLat, fmtLatency(t.p95),
		colLat, fmtLatency(t.p99),
	)
}

func fmtLatency(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	}
	if d < 10*time.Millisecond {
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.0fms", float64(d)/float64(time.Millisecond))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

// ─── JSON ────────────────────────────────────────────────────────────────────

type jsonReport struct {
	Workload  string       `json:"workload"`
	Workers   int          `json:"workers_per_target"`
	Targets   []jsonTarget `json:"targets"`
	Aggregate jsonTarget   `json:"aggregate"`
}

type jsonTarget struct {
	DSN     string  `json:"dsn"`
	Queries int     `json:"queries"`
	Errors  int     `json:"errors"`
	QPS     float64 `json:"qps"`
	P50Ms   float64 `json:"p50_ms"`
	P95Ms   float64 `json:"p95_ms"`
	P99Ms   float64 `json:"p99_ms"`
	MaxMs   float64 `json:"max_ms"`
}

func printJSON(r report, workloadLabel string, workersPerTarget int) {
	toJSONTarget := func(t targetStats) jsonTarget {
		return jsonTarget{
			DSN:     t.dsn,
			Queries: t.queries,
			Errors:  t.errors,
			QPS:     t.qps,
			P50Ms:   msFloat(t.p50),
			P95Ms:   msFloat(t.p95),
			P99Ms:   msFloat(t.p99),
			MaxMs:   msFloat(t.max),
		}
	}

	jr := jsonReport{
		Workload:  workloadLabel,
		Workers:   workersPerTarget,
		Targets:   make([]jsonTarget, len(r.targets)),
		Aggregate: toJSONTarget(r.aggregate),
	}
	for i, t := range r.targets {
		jr.Targets[i] = toJSONTarget(t)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(jr)
}

func msFloat(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// ─── CSV ─────────────────────────────────────────────────────────────────────

func printCSV(r report) {
	w := csv.NewWriter(os.Stdout)
	_ = w.Write([]string{"target", "queries", "errors", "qps", "p50_ms", "p95_ms", "p99_ms", "max_ms"})
	for _, t := range r.targets {
		_ = w.Write(csvRow(t))
	}
	if len(r.targets) > 1 {
		_ = w.Write(csvRow(r.aggregate))
	}
	w.Flush()
}

func csvRow(t targetStats) []string {
	return []string{
		t.dsn,
		strconv.Itoa(t.queries),
		strconv.Itoa(t.errors),
		fmt.Sprintf("%.2f", t.qps),
		fmt.Sprintf("%.3f", msFloat(t.p50)),
		fmt.Sprintf("%.3f", msFloat(t.p95)),
		fmt.Sprintf("%.3f", msFloat(t.p99)),
		fmt.Sprintf("%.3f", msFloat(t.max)),
	}
}
