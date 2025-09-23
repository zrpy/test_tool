// gotestload_fast.go
//
// High-performance authorized load tester (single-file).
// Build: go build -o gotestload_fast gotestload_fast.go
//
// Example:
//  ./gotestload_fast -url https://staging.example.gov/health -concurrency 200 -duration 30s -qps 10000 -confirm yes
//
// IMPORTANT: Only run against systems you are explicitly authorized to test.

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	mathrand "math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Stats struct {
	Total       int64            `json:"total"`
	Success     int64            `json:"success"`
	Failures    int64            `json:"failures"`
	StatusCodes map[int]int64    `json:"status_codes"`
	LatBuckets  map[string]int64 `json:"latency_buckets"`
	Start       time.Time        `json:"start"`
	End         time.Time        `json:"end"`
}

type result struct {
	status int
	err    string
	lat    time.Duration
}

func fileReadOrNil(path string) ([]byte, error) {
	if path == "" {
		return nil, nil
	}
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func main() {
	// Flags
	target := flag.String("url", "", "Target URL (required)")
	concurrency := flag.Int("concurrency", 100, "Number of concurrent workers (goroutines)")
	duration := flag.Duration("duration", 0, "Duration to run (e.g., 30s). If 0 you must set -requests")
	requests := flag.Int("requests", 0, "Total requests to send (ignored if -duration>0)")
	method := flag.String("method", "GET", "HTTP method")
	bodyFile := flag.String("body-file", "", "Path to body file (optional)")
	headersFlag := flag.String("header", "", "Headers as 'Key: Value' separated by \\n or 'Key:Value;Key2:Value2'")
	insecure := flag.Bool("insecure", false, "Skip TLS verification (insecure)")
	qps := flag.Int("qps", 0, "Global QPS limit (0 = unlimited)")
	timeout := flag.Duration("timeout", 10*time.Second, "Per-request timeout")
	confirm := flag.String("confirm", "no", "Type 'yes' to confirm you have permission")
	jsonOut := flag.Bool("json", false, "Output JSON summary")
	flag.Parse()

	if *target == "" {
		fmt.Fprintln(os.Stderr, "error: -url required")
		flag.Usage()
		os.Exit(2)
	}
	if strings.ToLower(*confirm) != "yes" {
		fmt.Fprintln(os.Stderr, "Aborting: you must set -confirm yes to acknowledge permission.")
		os.Exit(2)
	}

	if *duration == 0 && *requests == 0 {
		fmt.Fprintln(os.Stderr, "error: either -duration or -requests must be set")
		os.Exit(2)
	}

	// Basic limits to reduce accidental excessive usage
	*concurrency = clampInt(*concurrency, 1, 20000)
	if *qps < 0 {
		*qps = 0
	}

	// Read body
	body, err := fileReadOrNil(*bodyFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read body-file: %v\n", err)
		os.Exit(1)
	}

	// Parse headers
	headers := make(map[string]string)
	if *headersFlag != "" {
		// accept newline or semicolon separated
		parts := strings.FieldsFunc(*headersFlag, func(r rune) bool {
			return r == '\n' || r == ';'
		})
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			kv := strings.SplitN(p, ":", 2)
			if len(kv) != 2 {
				fmt.Fprintf(os.Stderr, "invalid header: %q\n", p)
				os.Exit(1)
			}
			headers[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}

	// Tune runtime for high concurrency
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Build tuned transport
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: *insecure},
		DisableCompression:    false,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          *concurrency * 50,
		MaxIdleConnsPerHost:   *concurrency * 50,
		IdleConnTimeout:       120 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ReadBufferSize:        0,
		WriteBufferSize:       0,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   *timeout,
	}

	// Prepare base request with GetBody (so Transport can recreate body)
	baseReq, err := http.NewRequest(*method, *target, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid url/method: %v\n", err)
		os.Exit(1)
	}
	for k, v := range headers {
		baseReq.Header.Set(k, v)
	}
	if body != nil {
		b := body // capture
		baseReq.GetBody = func() (io.ReadCloser, error) {
			return ioutil.NopCloser(bytes.NewReader(b)), nil
		}
		// set ContentLength
		baseReq.Body, _ = baseReq.GetBody()
		baseReq.ContentLength = int64(len(b))
	}

	// Channels
	jobs := make(chan *http.Request, (*concurrency)*4)
	results := make(chan result, (*concurrency)*4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SIGINT/SIGTERM handling for graceful shutdown
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case s := <-sigc:
			fmt.Fprintf(os.Stderr, "caught signal %v: shutting down\n", s)
			cancel()
		case <-ctx.Done():
		}
	}()

	// Token bucket for QPS (if qps>0)
	var tokenC <-chan time.Time
	var tick *time.Ticker
	if *qps > 0 {
		// For high qps, use sub-millisecond ticks with burst allowance
		// We'll compute interval as nanoseconds per token.
		intervalNs := int64(math.Max(1.0, float64(time.Second.Nanoseconds())/float64(*qps)))
		tick = time.NewTicker(time.Duration(intervalNs))
		tokenC = tick.C
		defer tick.Stop()
	}

	var sent int64
	var totalToSend int64 = int64(*requests)
	useDuration := *duration > 0
	startTime := time.Now()

	// Worker function
	var wg sync.WaitGroup
	workerCount := *concurrency
	if workerCount < 1 {
		workerCount = 1
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// per-goroutine rand to avoid contention
			rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano() + int64(id*7919)))
			for {
				select {
				case <-ctx.Done():
					return
				case req, ok := <-jobs:
					if !ok {
						return
					}
					// optionally mutate UA per request using rng if header contains placeholder
					if v, ok := req.Header["User-Agent"]; ok && len(v) > 0 && strings.Contains(v[0], "{rand}") {
						ua := fmt.Sprintf("go-loadtester/%d-%d", id, rng.Intn(1000000))
						req.Header.Set("User-Agent", ua)
					}
					start := time.Now()
					resp, err := client.Do(req)
					lat := time.Since(start)
					if err != nil {
						results <- result{status: 0, err: err.Error(), lat: lat}
						// ensure body closed if any
						continue
					}
					// Drain and close
					n, _ := io.Copy(ioutil.Discard, resp.Body)
					_ = n
					resp.Body.Close()
					results <- result{status: resp.StatusCode, err: "", lat: lat}
				}
			}
		}(i)
	}

	// Collector
	var total int64
	var success int64
	var failures int64
	statusCounts := sync.Map{}
	// latency buckets: <1ms, <5ms, <10ms, <50ms, <100ms, <250ms, <500ms, <1s, >1s
	latThresholds := []time.Duration{1 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, 1 * time.Second}
	latHist := make([]int64, len(latThresholds)+1)
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for res := range results {
			atomic.AddInt64(&total, 1)
			if res.err != "" || res.status == 0 {
				atomic.AddInt64(&failures, 1)
			} else {
				atomic.AddInt64(&success, 1)
			}
			if res.status != 0 {
				v, _ := statusCounts.LoadOrStore(res.status, new(int64))
				atomic.AddInt64(v.(*int64), 1)
			}
			placed := false
			for i, t := range latThresholds {
				if res.lat <= t {
					atomic.AddInt64(&latHist[i], 1)
					placed = true
					break
				}
			}
			if !placed {
				atomic.AddInt64(&latHist[len(latHist)-1], 1)
			}
		}
	}()

	// Producer goroutine
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		for {
			// termination conditions
			if ctx.Err() != nil {
				return
			}
			if useDuration {
				if time.Since(startTime) >= *duration {
					return
				}
			} else if totalToSend > 0 {
				if atomic.LoadInt64(&sent) >= totalToSend {
					return
				}
			} else {
				return
			}

			// QPS control
			if tokenC != nil {
				select {
				case <-ctx.Done():
					return
				case <-tokenC:
					// proceed (one token)
				}
			}

			// Make a clone of baseReq
			req := baseReq.Clone(context.Background())
			// If GetBody exists, ensure Body is set (Clone doesn't set Body)
			if baseReq.GetBody != nil {
				r, _ := baseReq.GetBody()
				req.Body = r
			}
			// send job
			select {
			case jobs <- req:
				atomic.AddInt64(&sent, 1)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for producer and then cleanup
	<-prodDone
	// close jobs (workers will finish)
	close(jobs)
	// wait workers
	wg.Wait()
	// close results and wait collector
	close(results)
	<-collectorDone
	endTime := time.Now()

	// Build stats
	stats := Stats{
		Total:       atomic.LoadInt64(&total),
		Success:     atomic.LoadInt64(&success),
		Failures:    atomic.LoadInt64(&failures),
		StatusCodes: make(map[int]int64),
		LatBuckets:  make(map[string]int64),
		Start:       startTime,
		End:         endTime,
	}
	statusCounts.Range(func(k, v interface{}) bool {
		code := k.(int)
		cnt := atomic.LoadInt64(v.(*int64))
		stats.StatusCodes[code] = cnt
		return true
	})
	labels := []string{"<=1ms", "<=5ms", "<=10ms", "<=50ms", "<=100ms", "<=250ms", "<=500ms", "<=1s", ">1s"}
	for i, l := range labels {
		stats.LatBuckets[l] = atomic.LoadInt64(&latHist[i])
	}

	// Print summary
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(stats)
	} else {
		fmt.Println("========== RESULT SUMMARY ==========")
		fmt.Printf("Target: %s\n", *target)
		fmt.Printf("Concurrency: %d  Sent: %d\n", *concurrency, atomic.LoadInt64(&sent))
		fmt.Printf("Duration: %v → %v\n", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
		fmt.Printf("Total processed: %d\n", stats.Total)
		fmt.Printf("Success: %d  Failures: %d\n", stats.Success, stats.Failures)
		fmt.Println("\nStatus codes:")
		// sort codes
		var codes []int
		for c := range stats.StatusCodes {
			codes = append(codes, c)
		}
		sort.Ints(codes)
		for _, c := range codes {
			fmt.Printf("  %d : %d\n", c, stats.StatusCodes[c])
		}
		fmt.Println("\nLatency buckets:")
		for _, l := range labels {
			fmt.Printf("  %s : %d\n", l, stats.LatBuckets[l])
		}
		fmt.Println("====================================")
	}
	// Best-effort cleanup
	tr.CloseIdleConnections()
}
