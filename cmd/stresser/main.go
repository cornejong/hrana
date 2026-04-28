package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/cornejong/hrana/clients/hranago"
)

// dsnList is a repeatable --dsn flag value.
type dsnList []string

func (d *dsnList) String() string {
	if len(*d) == 0 {
		return ""
	}
	return strings.Join(*d, ", ")
}

func (d *dsnList) Set(v string) error {
	*d = append(*d, v)
	return nil
}

func main() {
	var dsns dsnList
	flag.Var(&dsns, "dsn", "Hrana DSN, e.g. http://localhost:8080?token=secret (repeatable)")
	workload := flag.String("workload", "ping", "ping | read | write | mixed | tx")
	readRatio := flag.Float64("read-ratio", 0.8, "fraction of reads in mixed workload (0.0–1.0)")
	rows := flag.Int("rows", 1000, "rows to seed / use as ID range")
	workers := flag.Int("workers", 10, "concurrent connections per DSN target")
	duration := flag.Duration("duration", 30*time.Second, "measurement duration, e.g. 30s, 2m")
	warmup := flag.Duration("warmup", 5*time.Second, "warm-up period; samples are discarded")
	thinkTime := flag.Duration("think-time", 0, "pause between queries per worker (0 = none)")
	output := flag.String("output", "table", "table | json | csv")
	keepSchema := flag.Bool("keep-schema", false, "do not drop stress_kv table after run")
	flag.Parse()

	if len(dsns) == 0 {
		fmt.Fprintln(os.Stderr, "error: at least one --dsn is required")
		flag.Usage()
		os.Exit(1)
	}

	fn, err := resolveWorkload(*workload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if *readRatio < 0 || *readRatio > 1 {
		fmt.Fprintln(os.Stderr, "error: --read-ratio must be between 0.0 and 1.0")
		os.Exit(1)
	}

	if *rows <= 0 && needsRows(*workload) {
		fmt.Fprintln(os.Stderr, "error: --rows must be > 0 for this workload")
		os.Exit(1)
	}

	switch *output {
	case "table", "json", "csv":
	default:
		fmt.Fprintf(os.Stderr, "error: --output must be one of: table, json, csv\n")
		os.Exit(1)
	}

	// Open one *sql.DB per DSN.
	dbs := make([]*sql.DB, len(dsns))
	for i, dsn := range dsns {
		db, openErr := sql.Open("hrana", dsn)
		if openErr != nil {
			fmt.Fprintf(os.Stderr, "error: open %s: %v\n", dsn, openErr)
			os.Exit(1)
		}
		db.SetMaxOpenConns(*workers)
		db.SetMaxIdleConns(*workers)
		dbs[i] = db
	}
	defer func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	}()

	bgCtx := context.Background()

	// Set up schema in parallel across all targets.
	setupErrs := make([]error, len(dbs))
	var setupWG sync.WaitGroup
	for i := range dbs {
		setupWG.Add(1)
		go func(idx int) {
			defer setupWG.Done()
			fmt.Fprintf(os.Stderr, "setting up schema for %s...\n", dsns[idx])
			if sErr := setupSchema(bgCtx, dbs[idx]); sErr != nil {
				setupErrs[idx] = fmt.Errorf("schema %s: %w", dsns[idx], sErr)
				return
			}
			if needsSeed(*workload) {
				fmt.Fprintf(os.Stderr, "seeding %d rows for %s...\n", *rows, dsns[idx])
				if sErr := seedSchema(bgCtx, dbs[idx], *rows); sErr != nil {
					setupErrs[idx] = fmt.Errorf("seed %s: %w", dsns[idx], sErr)
				}
			}
		}(i)
	}
	setupWG.Wait()

	for _, sErr := range setupErrs {
		if sErr != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", sErr)
			os.Exit(1)
		}
	}

	// Shared results channel; a single collector goroutine reads it.
	totalWorkers := len(dsns) * *workers
	results := make(chan sample, totalWorkers*64)

	collected := make(map[string][]sample, len(dsns))
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for s := range results {
			collected[s.dsn] = append(collected[s.dsn], s)
		}
	}()

	// Run context spans warmup + measurement.
	runCtx, cancel := context.WithTimeout(bgCtx, *warmup+*duration)
	defer cancel()
	warmupEnd := time.Now().Add(*warmup)

	wCfg := workloadCfg{rows: *rows, readRatio: *readRatio}

	fmt.Fprintf(os.Stderr, "running %s workload for %s (warmup %s)...\n", *workload, *duration, *warmup)

	var workerWG sync.WaitGroup
	for i, dsn := range dsns {
		for range *workers {
			workerWG.Add(1)
			go func(dsn string, db *sql.DB) {
				defer workerWG.Done()
				conn, connErr := db.Conn(runCtx)
				if connErr != nil {
					fmt.Fprintf(os.Stderr, "warn: connect %s: %v\n", dsn, connErr)
					return
				}
				defer conn.Close()
				rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
				runWorker(runCtx, conn, dsn, fn, wCfg, warmupEnd, *thinkTime, results, rng)
			}(dsn, dbs[i])
		}
	}

	// Wait for the timeout, then drain workers and collector.
	<-runCtx.Done()
	workerWG.Wait()
	close(results)
	<-collectorDone

	// Print report.
	r := buildReport(collected, []string(dsns), *duration)
	printReport(r, *output, workloadLabel(*workload, *readRatio), *workers, *duration, *warmup)

	// Teardown unless --keep-schema.
	if !*keepSchema {
		for i, db := range dbs {
			if tErr := teardownSchema(bgCtx, db); tErr != nil {
				fmt.Fprintf(os.Stderr, "warn: teardown %s: %v\n", dsns[i], tErr)
			}
		}
	}
}

func runWorker(
	ctx context.Context,
	conn *sql.Conn,
	dsn string,
	fn workloadFn,
	cfg workloadCfg,
	warmupEnd time.Time,
	thinkTime time.Duration,
	results chan<- sample,
	rng *rand.Rand,
) {
	for ctx.Err() == nil {
		start := time.Now()
		err := fn(ctx, conn, rng, cfg)
		elapsed := time.Since(start)

		// Discard samples caused by context cancellation at shutdown.
		if ctx.Err() != nil {
			return
		}

		if time.Now().After(warmupEnd) {
			results <- sample{dsn: dsn, latency: elapsed, err: err}
		}

		if thinkTime > 0 {
			t := time.NewTimer(thinkTime)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}
}

func workloadLabel(workload string, readRatio float64) string {
	if workload == "mixed" {
		return fmt.Sprintf("mixed (read %.0f%% / write %.0f%%)", readRatio*100, (1-readRatio)*100)
	}
	return workload
}
