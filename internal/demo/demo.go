// Package demo drives a running stack through the whole product path: wait for
// readiness, load a small deterministic dataset, run a representative query,
// and check the answer against values computed independently of the engine.
//
// It only ever touches the dataset named DatasetName, never deletes anything,
// and is safe to rerun: a dataset that is already loaded is reused.
package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// DatasetName is the only dataset the demo creates or reads.
const DatasetName = "demo-sales"

// Statement is the representative query.
const Statement = `SELECT region, count(*) AS orders, sum(quantity * unit_price) AS revenue ` +
	`FROM "demo-sales" GROUP BY region ORDER BY revenue DESC`

// Options configures a run.
type Options struct {
	API     string        // API base URL, e.g. http://127.0.0.1:8080
	Web     string        // dashboard URL shown to the user
	Token   string        // optional bearer token
	Timeout time.Duration // overall budget
	Poll    time.Duration // poll interval (default 1s)
	Rows    int           // sample size (default 2000)
	Client  *http.Client
}

var regions = []string{"AMER", "APAC", "EMEA", "LATAM"}
var products = []string{"Atlas", "Beacon", "Cobalt", "Delta", "Ember"}

// Expected is what the sample data must produce, computed in Go.
type Expected struct {
	Orders  map[string]int64
	Revenue map[string]float64
}

// Sample returns the deterministic CSV and its expected aggregates. The same
// rows are produced on every run, so reruns are comparable.
func Sample(rows int) ([]byte, Expected) {
	rng := rand.New(rand.NewPCG(8, 80))
	exp := Expected{Orders: map[string]int64{}, Revenue: map[string]float64{}}
	var b bytes.Buffer
	b.WriteString("order_id,region,product,quantity,unit_price,order_date\n")
	for i := 1; i <= rows; i++ {
		region := regions[rng.IntN(len(regions))]
		product := products[rng.IntN(len(products))]
		qty := 1 + rng.IntN(20)
		cents := 900 + rng.IntN(23_000) // 9.00 .. 238.99
		fmt.Fprintf(&b, "%d,%s,%s,%d,%d.%02d,2025-%02d-%02d\n", i, region, product, qty, cents/100, cents%100, 1+rng.IntN(12), 1+rng.IntN(28))
		exp.Orders[region]++
		exp.Revenue[region] += float64(qty*cents) / 100
	}
	return b.Bytes(), exp
}

type runner struct {
	o   Options
	out io.Writer
	c   *http.Client
}

func (r *runner) logf(format string, a ...any) { _, _ = fmt.Fprintf(r.out, format+"\n", a...) }

func (r *runner) do(ctx context.Context, method, path string, body io.Reader, ctype string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(r.o.API, "/")+path, body)
	if err != nil {
		return 0, err
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if method != http.MethodGet {
		req.Header.Set("X-Lakehouse-CSRF", "1") // mandatory proof on every mutation
	}
	if r.o.Token != "" {
		req.Header.Set("Authorization", "Bearer "+r.o.Token)
	}
	resp, err := r.c.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct{ Code, Message string }
		}
		_ = json.Unmarshal(data, &e)
		return resp.StatusCode, fmt.Errorf("%s %s: %d %s: %s", method, path, resp.StatusCode, e.Error.Code, e.Error.Message)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: bad response: %w", method, path, err)
		}
	}
	return resp.StatusCode, nil
}

func (r *runner) sleep(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(r.o.Poll):
		return nil
	}
}

// readyz fetches /readyz. The API answers 503 *with* per-dependency detail when
// something is down, so the body is read for any status.
func (r *runner) readyz(ctx context.Context) (ok bool, detail string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(r.o.API, "/")+"/readyz", nil)
	if err != nil {
		return false, err.Error()
	}
	if r.o.Token != "" {
		req.Header.Set("Authorization", "Bearer "+r.o.Token)
	}
	resp, err := r.c.Do(req)
	if err != nil {
		return false, "the API is not reachable yet"
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var ready struct {
		Status       string
		Dependencies map[string]struct {
			OK      bool
			Message string
		}
	}
	if err := json.Unmarshal(data, &ready); err != nil {
		return false, fmt.Sprintf("readiness answered %d", resp.StatusCode)
	}
	if ready.Status == "ok" {
		return true, ""
	}
	names := make([]string, 0, len(ready.Dependencies))
	for name := range ready.Dependencies {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if d := ready.Dependencies[name]; !d.OK {
			return false, fmt.Sprintf("%s is not ready (%s)", name, d.Message)
		}
	}
	return false, fmt.Sprintf("readiness answered %d", resp.StatusCode)
}

func (r *runner) waitReady(ctx context.Context) error {
	r.logf("• waiting for the stack to become ready (%s)...", r.o.API)
	var last string
	for {
		ok, detail := r.readyz(ctx)
		if ok {
			r.logf("  ready")
			return nil
		}
		if ctx.Err() != nil {
			// The budget ran out mid-request: keep the last informative reason.
			return fmt.Errorf("stack did not become ready in time: %s", last)
		}
		last = detail
		if err := r.sleep(ctx); err != nil {
			return fmt.Errorf("stack did not become ready in time: %s", last)
		}
	}
}

type ingestJob struct {
	ID           string `json:"id"`
	DatasetName  string `json:"datasetName"`
	Status       string `json:"status"`
	ErrorSummary string `json:"errorSummary"`
}

// alreadyLoaded reports whether a successful ingestion of the demo dataset exists.
func (r *runner) alreadyLoaded(ctx context.Context) (bool, error) {
	var list struct{ Jobs []ingestJob }
	if _, err := r.do(ctx, http.MethodGet, "/api/v1/ingestions", nil, "", &list); err != nil {
		return false, err
	}
	for _, j := range list.Jobs {
		if j.DatasetName == DatasetName && j.Status == "SUCCEEDED" {
			return true, nil
		}
	}
	return false, nil
}

func (r *runner) load(ctx context.Context, csv []byte) error {
	q := url.Values{"name": {DatasetName}, "filename": {"sales.csv"}}
	var job ingestJob
	if _, err := r.do(ctx, http.MethodPost, "/api/v1/ingestions?"+q.Encode(), bytes.NewReader(csv), "text/csv", &job); err != nil {
		return err
	}
	r.logf("  upload accepted (job %s); converting to Parquet...", job.ID[:8])
	for {
		if err := r.sleep(ctx); err != nil {
			return errors.New("ingestion did not finish in time")
		}
		if _, err := r.do(ctx, http.MethodGet, "/api/v1/ingestions/"+job.ID, nil, "", &job); err != nil {
			return err
		}
		switch job.Status {
		case "SUCCEEDED":
			return nil
		case "FAILED", "CANCELLED":
			return fmt.Errorf("ingestion %s: %s", strings.ToLower(job.Status), job.ErrorSummary)
		}
	}
}

type queryJob struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	FailureReason string `json:"failureReason"`
	ErrorSummary  string `json:"errorSummary"`
	DurationMs    *int64 `json:"durationMs"`
	RowsReturned  *int64 `json:"rowsReturned"`
}

type results struct {
	Columns []struct{ Name, Type string }
	Rows    [][]any
}

func (r *runner) query(ctx context.Context) (queryJob, results, error) {
	body, _ := json.Marshal(map[string]string{"statement": Statement})
	var job queryJob
	if _, err := r.do(ctx, http.MethodPost, "/api/v1/queries", bytes.NewReader(body), "application/json", &job); err != nil {
		return job, results{}, err
	}
	for {
		switch job.Status {
		case "SUCCEEDED":
			var res results
			_, err := r.do(ctx, http.MethodGet, "/api/v1/queries/"+job.ID+"/results?limit=100", nil, "", &res)
			return job, res, err
		case "FAILED", "CANCELLED", "TIMED_OUT":
			return job, results{}, fmt.Errorf("query %s (%s): %s", strings.ToLower(job.Status), job.FailureReason, job.ErrorSummary)
		}
		if err := r.sleep(ctx); err != nil {
			return job, results{}, errors.New("query did not finish in time")
		}
		if _, err := r.do(ctx, http.MethodGet, "/api/v1/queries/"+job.ID, nil, "", &job); err != nil {
			return job, results{}, err
		}
	}
}

func asFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	}
	return 0, false
}

// check compares the query result with the independently computed expectation.
func check(res results, exp Expected) error {
	if len(res.Rows) != len(exp.Orders) {
		return fmt.Errorf("got %d regions, want %d", len(res.Rows), len(exp.Orders))
	}
	for _, row := range res.Rows {
		if len(row) != 3 {
			return fmt.Errorf("unexpected row shape %v", row)
		}
		region, _ := row[0].(string)
		orders, ok1 := asFloat(row[1])
		rev, ok2 := asFloat(row[2])
		if !ok1 || !ok2 {
			return fmt.Errorf("non-numeric result for %s: %v", region, row)
		}
		if int64(orders) != exp.Orders[region] {
			return fmt.Errorf("%s: %d orders, want %d", region, int64(orders), exp.Orders[region])
		}
		if math.Abs(rev-exp.Revenue[region]) > 0.01 {
			return fmt.Errorf("%s: revenue %.2f, want %.2f", region, rev, exp.Revenue[region])
		}
	}
	return nil
}

func (r *runner) print(res results, job queryJob) {
	tw := tabwriter.NewWriter(r.out, 2, 4, 3, ' ', tabwriter.AlignRight)
	for i, c := range res.Columns {
		if i > 0 {
			_, _ = fmt.Fprint(tw, "\t")
		}
		_, _ = fmt.Fprint(tw, strings.ToUpper(c.Name))
	}
	_, _ = fmt.Fprintln(tw, "\t")
	rows := res.Rows
	for _, row := range rows {
		for i, v := range row {
			if i > 0 {
				_, _ = fmt.Fprint(tw, "\t")
			}
			if f, ok := v.(float64); ok && f != math.Trunc(f) {
				_, _ = fmt.Fprintf(tw, "%.2f", f)
			} else if f, ok := v.(float64); ok {
				_, _ = fmt.Fprintf(tw, "%d", int64(f))
			} else {
				_, _ = fmt.Fprint(tw, v)
			}
		}
		_, _ = fmt.Fprintln(tw, "\t")
	}
	_ = tw.Flush()
	d := "?"
	if job.DurationMs != nil {
		d = fmt.Sprintf("%d ms", *job.DurationMs)
	}
	r.logf("  %d rows in %s", len(res.Rows), d)
}

// Run executes the demo, writing progress to out.
func Run(ctx context.Context, o Options, out io.Writer) error {
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Minute
	}
	if o.Poll <= 0 {
		o.Poll = time.Second
	}
	if o.Rows <= 0 {
		o.Rows = 2000
	}
	if o.Client == nil {
		o.Client = &http.Client{Timeout: 30 * time.Second}
	}
	r := &runner{o: o, out: out, c: o.Client}
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()

	if err := r.waitReady(ctx); err != nil {
		return err
	}

	csv, exp := Sample(o.Rows)
	loaded, err := r.alreadyLoaded(ctx)
	if err != nil {
		return err
	}
	if loaded {
		r.logf("• sample dataset %q is already loaded; reusing it (nothing is deleted or duplicated)", DatasetName)
	} else {
		r.logf("• loading the sample dataset %q (%d rows)...", DatasetName, o.Rows)
		if err := r.load(ctx, csv); err != nil {
			return err
		}
		r.logf("  dataset is READY")
	}

	r.logf("• running a representative query:")
	r.logf("    %s", Statement)
	job, res, err := r.query(ctx)
	if err != nil {
		return err
	}
	r.print(res, job)

	if err := check(res, exp); err != nil {
		if loaded {
			r.logf("  note: result differs from the bundled sample (%v); a dataset named %q with other data already exists", err, DatasetName)
		} else {
			return fmt.Errorf("result does not match the independently computed expectation: %w", err)
		}
	} else {
		r.logf("  verified against values computed independently of the query engine")
	}

	web := strings.TrimRight(o.Web, "/")
	r.logf("")
	r.logf("Demo complete.")
	r.logf("  Dashboard:  %s/", web)
	r.logf("  SQL editor: %s/#/query   (try: %s)", web, Statement)
	r.logf("  Datasets:   %s/#/datasets", web)
	r.logf("")
	r.logf("Cleanup: `make down` stops the stack and keeps your data. Nothing was deleted.")
	r.logf("         `docker compose down -v` also deletes the stack's volumes (all data) — run it only if you want that.")
	return nil
}
