// Command demo loads a deterministic sample dataset into a running stack, runs
// a representative query, verifies the answer, and prints where to look in the
// dashboard. It is what `make demo` runs after starting the stack.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/demo"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	api := flag.String("api", "http://127.0.0.1:"+env("API_HOST_PORT", "8080"), "API base URL")
	web := flag.String("web", "http://localhost:"+env("WEB_HOST_PORT", "5173"), "dashboard URL to print")
	timeout := flag.Duration("timeout", 5*time.Minute, "overall time budget")
	rows := flag.Int("rows", 2000, "rows in the sample dataset")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := demo.Run(ctx, demo.Options{API: *api, Web: *web, Token: os.Getenv("API_TOKEN"), Timeout: *timeout, Rows: *rows}, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "\ndemo failed:", err)
		os.Exit(1)
	}
}
