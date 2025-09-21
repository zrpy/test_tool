// gotestload.go
// Simple, configurable load tester for legal/authorized testing.
//
// Build: go build -o gotestload gotestload.go
// Example:
//   ./gotestload -url https://test.example.gov/endpoint -concurrency 50 -requests 5000 -method POST -body-file payload.json -qps 200
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Result struct {
	status int
	err    error
	lat    time.Duration
}

func worker(ctx context.Context, id int, client *http.Client, jobs <-chan *http.Request, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-jobs:
			if !ok {
				return
			}
			start := time.Now()
			resp, err := client.Do(req)
			lat := time.Since(start)
			status := 0
			if err == nil {
				status = resp.StatusCode
				// Drain body to allow connection reuse
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			results <- Result{status: status, err: err, lat: lat}
		}
	}
}

func makeRequest(method, url string, headers map[string]string, body []byte) (*http.Request, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

func readFileOrNil(path string) ([]byte, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

func main() {
	// Flags
	url := flag.String("url", "", "Target URL (required)")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	requests := flag.Int("requests", 0, "Total number of requests to send (0 means use -duration)")
	duration := flag.Duration("duration", 0, "Total duration to run (e.g. 30s). If >0, -requests is ignored.")
	method := flag.String("method", "GET", "HTTP method")
	headersFlag := flag.String("header", "", "Extra headers (comma-separated, e.g. 'Authorization:Bearer x, X-Env:stg')")
	bodyFile := flag.String("body-file", "", "Path to file to use as request body (optional)")
	insecure := flag.Bool("insecure", false, "Allow insecure TLS (skip cert verify)")
	qps := flag.Int("qps", 0, "Global QPS limit (0 means unlimited). QPS is approximate.")
	timeout := flag.Duration("timeout", 10*time.Second, "Per-request timeout")
	showHelp := flag.Bool("help", false, "Show help")
	flag.Parse()

	if *showHelp || *url == "" {
		flag.Usage()
		fmt.Println("\nIMPORTANT: Only run this tool against systems you are explicitly authorized to test.")
		return
	}

	// Parse headers
	headers := map[string]string{}
	if *headersFlag != "" {
		parts := strings.Split(*headersFlag, ",")
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

	body, err := readFileOrNil(*bodyFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read body file: %v\n", err)
		os.Exit(1)
	}

	// HTTP client with tuned Transport for high concurrency
	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: *insecure},
		DisableCompression:  false,
		MaxIdleConns:        *concurrency * 4,
		MaxIdleConnsPerHost: *concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   *timeout,
	}

	// Channels and orchestration
	jobs := make(chan *http.Request, *concurrency*2)
	results := make(chan Result, *concurrency*2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(ctx, i, client, jobs, results, &wg)
	}

	// Producer: generate requests according to requests or duration, enforce qps if set
	var totalToSend int64 = int64(*requests)
	var useDuration bool = *duration > 0
	var sent int64

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		startTime := time.Now()
		if *qps > 0 {
			interval := time.Second / time.Duration(*qps)
			if interval <= 0 {
				interval = time.Nanosecond
			}
			t := time.NewTicker(interval)
			defer t.Stop()
		}
		for {
			// Termination checks
			if useDuration {
				if time.Since(startTime) >= *duration {
					return
				}
			} else if totalToSend > 0 {
				if atomic.LoadInt64(&sent) >= totalToSend {
					return
				}
			} else {
				// neither requests nor duration specified — nothing to do
				return
			}

			// If QPS limited, wait for tick
			if tickC != nil {
				select {
				case <-ctx.Done():
					return
				case <-tickC:
					// proceed
				}
			}

			// Build request
			req, err := makeRequest(*method, *url, headers, body)
			if err != nil {
				// push an error result (to be recorded)
				results <- Result{status: 0, err: err, lat: 0}
				// still count it so we don't loop forever on bad input
				atomic.AddInt64(&sent, 1)
				continue
			}

			// Send job (block if buffer full)
			select {
			case jobs <- req:
				atomic.AddInt64(&sent, 1)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collector goroutine
	var total int64
	var success int64
	var failures int64
	var statusCounts sync.Map // map[int]int64
	latBuckets := []time.Duration{
		10 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		5 * time.Second,
	}
	latHist := make([]int64, len(latBuckets)+1)

	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for res := range results {
			atomic.AddInt64(&total, 1)
			if res.err != nil {
				atomic.AddInt64(&failures, 1)
			} else {
				atomic.AddInt64(&success, 1)
			}
			// status counts
			if res.status != 0 {
				v, _ := statusCounts.LoadOrStore(res.status, new(int64))
				ptr := v.(*int64)
				atomic.AddInt64(ptr, 1)
			}
			// latency histogram
			l := res.lat
			placed := false
			for i, b := range latBuckets {
				if l <= b {
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

	// Wait for producer to finish
	<-producerDone
	// Close jobs to stop workers once they finish queued work
	close(jobs)
	// Wait workers to finish
	wg.Wait()
	// All workers done; close results
	close(results)
	// Wait collector done
	<-collectorDone

	// Print summary
	fmt.Println("========== RESULT SUMMARY ==========")
	fmt.Printf("Target: %s\n", *url)
	fmt.Printf("Concurrency: %d\n", *concurrency)
	if useDuration {
		fmt.Printf("Duration: %v\n", *duration)
	} else {
		fmt.Printf("Total requested: %d\n", sent)
	}
	fmt.Printf("Total requests processed: %d\n", atomic.LoadInt64(&total))
	fmt.Printf("Success: %d  Failures: %d\n", atomic.LoadInt64(&success), atomic.LoadInt64(&failures))

	// status codes
	fmt.Println("\nStatus codes:")
	statusCounts.Range(func(k, v interface{}) bool {
		code := k.(int)
		cnt := atomic.LoadInt64(v.(*int64))
		fmt.Printf("  %d: %d\n", code, cnt)
		return true
	})

	// latencies
	fmt.Println("\nLatency histogram (<=bucket):")
	var cumulative int64
	for i, b := range latBuckets {
		cnt := atomic.LoadInt64(&latHist[i])
		cumulative += cnt
		fmt.Printf("  <= %v : %d\n", b, cnt)
	}
	// last bucket
	lastCnt := atomic.LoadInt64(&latHist[len(latHist)-1])
	fmt.Printf("  > %v : %d\n", latBuckets[len(latBuckets)-1], lastCnt)

	// Some basic percentiles (approx)
	// We have only histogram; compute approx percentiles
	totalCount := atomic.LoadInt64(&total)
	if totalCount > 0 {
		targets := []float64{50, 90, 95, 99}
		cum := int64(0)
		idx := 0
		for _, p := range targets {
			want := int64(float64(totalCount) * p / 100.0)
			// advance until cumulative >= want
			for idx < len(latHist) && cum < want {
				cum += atomic.LoadInt64(&latHist[idx])
				idx++
			}
			var bucketLabel string
			if idx-1 < len(latBuckets) {
				if idx-1 < 0 {
					bucketLabel = fmt.Sprintf("<= %v", latBuckets[0])
				} else {
					bucketLabel = fmt.Sprintf("<= %v", latBuckets[idx-1])
				}
			} else {
				bucketLabel = fmt.Sprintf("> %v", latBuckets[len(latBuckets)-1])
			}
			fmt.Printf("  approx %vth percentile: %s\n", p, bucketLabel)
		}
	}

	fmt.Println("====================================")
	fmt.Println("NOTE: This tool is for authorized testing only. Ensure you have written permission before testing any live/production service.")
}
