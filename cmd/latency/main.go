// Command latency measures how long a statement takes to run on a live stack,
// split into queue wait (submitted until a worker claims it), worker time
// (claim to finished) and the total the client sees. It talks to the public
// HTTP API only, so it measures the real path: API, PostgreSQL, worker,
// object store.
//
//	go run ./cmd/latency -n 20 -sql 'SELECT 1 AS one'
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"
)

type job struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	DurationMs *int64 `json:"durationMs"`
	CreatedAt  string `json:"createdAt"`
	StartedAt  string `json:"startedAt"`
	FinishedAt string `json:"finishedAt"`
	Error      string `json:"errorSummary"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "latency:", err)
		os.Exit(1)
	}
}

func run() error {
	base := flag.String("api", "http://127.0.0.1:8080", "API base URL")
	stmt := flag.String("sql", "SELECT 1 AS one", "statement to run")
	n := flag.Int("n", 20, "number of runs")
	token := flag.String("token", os.Getenv("API_TOKEN"), "bearer token, if the API requires one")
	flag.Parse()

	client := &http.Client{Timeout: 30 * time.Second}
	call := func(method, path string, body any) (job, error) {
		var rd io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = bytes.NewReader(b)
		}
		req, _ := http.NewRequest(method, *base+path, rd)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Lakehouse-CSRF", "1")
		if *token != "" {
			req.Header.Set("Authorization", "Bearer "+*token)
		}
		resp, err := client.Do(req)
		if err != nil {
			return job{}, err
		}
		defer func() { _ = resp.Body.Close() }()
		data, _ := io.ReadAll(resp.Body)
		var j job
		if err := json.Unmarshal(data, &j); err != nil || resp.StatusCode >= 300 {
			return j, fmt.Errorf("%s %s: HTTP %d", method, path, resp.StatusCode)
		}
		return j, nil
	}
	ts := func(s string) time.Time { t, _ := time.Parse(time.RFC3339Nano, s); return t }

	var queue, work, total []float64
	for i := 0; i < *n; i++ {
		t0 := time.Now()
		j, err := call("POST", "/api/v1/queries", map[string]string{"statement": *stmt})
		if err != nil {
			return err
		}
		for j.Status != "SUCCEEDED" {
			if j, err = call("GET", "/api/v1/queries/"+j.ID+"?waitMs=3000", nil); err != nil {
				return err
			}
			if j.Status == "FAILED" || j.Status == "CANCELLED" || j.Status == "TIMED_OUT" {
				return fmt.Errorf("query %s: %s", j.Status, j.Error)
			}
		}
		total = append(total, float64(time.Since(t0).Microseconds())/1000)
		queue = append(queue, float64(ts(j.StartedAt).Sub(ts(j.CreatedAt)).Microseconds())/1000)
		work = append(work, float64(ts(j.FinishedAt).Sub(ts(j.StartedAt)).Microseconds())/1000)
	}
	row := func(name string, v []float64) {
		sort.Float64s(v)
		fmt.Printf("%-34s p50 %7.1f ms   p95 %7.1f ms   max %7.1f ms\n", name, v[len(v)/2], v[min(len(v)-1, len(v)*95/100)], v[len(v)-1])
	}
	fmt.Printf("%d runs of: %s\n", *n, *stmt)
	row("queue wait (submit -> claimed)", queue)
	row("worker (claimed -> finished)", work)
	row("client total (POST -> SUCCEEDED)", total)
	return nil
}
