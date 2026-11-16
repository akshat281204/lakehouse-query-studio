//go:build integration

package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// Runs the real client (hand-written SigV4) against a real S3-compatible
// server. Opt-in: set TEST_S3_ENDPOINT, TEST_S3_ACCESS_KEY, TEST_S3_SECRET_KEY.
// Any S3-compatible store works (MinIO, SeaweedFS, ...).

func s3Client(t *testing.T, secret string) *Client {
	t.Helper()
	ep := os.Getenv("TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("TEST_S3_ENDPOINT not set")
	}
	if secret == "" {
		secret = os.Getenv("TEST_S3_SECRET_KEY")
	}
	return New(Config{Endpoint: ep, AccessKey: os.Getenv("TEST_S3_ACCESS_KEY"), SecretKey: secret,
		Bucket: fmt.Sprintf("lqs-it-%d", time.Now().UnixNano()%1_000_000_000), Region: "us-east-1"})
}

func TestS3RealStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := s3Client(t, "")
	if err := c.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
	if err := c.EnsureBucket(ctx); err != nil { // idempotent
		t.Fatalf("EnsureBucket again: %v", err)
	}

	payload := []byte(strings.Repeat("parquet-ish bytes ", 1000))
	keys := []string{
		"lake/default/sales/version=1/attempt=1/part-0.parquet",
		"lake/default/sales/version=1/attempt=1/region=eu west/part-1.parquet", // space needs encoding
		"lake/default/sales/version=1/attempt=1/city=Zürich/part-2.parquet",    // non-ASCII
		"lake/default/sales/version=1/attempt=1/q=a%2Fb+c/part-3.parquet",      // literal % and +
		"uploads/default/sales/job/input.csv",
	}
	for _, k := range keys {
		if err := c.Put(ctx, k, payload); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}

	got, err := c.Get(ctx, keys[0], 1<<20)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("Get: %v (len %d)", err, len(got))
	}
	for _, k := range keys[1:4] {
		if b, err := c.Get(ctx, k, 1<<20); err != nil || len(b) != len(payload) {
			t.Fatalf("Get %q: %v", k, err)
		}
	}
	if _, err := c.Get(ctx, keys[0], 100); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("bounded Get: %v", err)
	}
	if _, err := c.Get(ctx, "does/not/exist", 100); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing key: %v", err)
	}

	rc, err := c.Range(ctx, keys[0], 5, 10)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	part, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(part) != string(payload[5:15]) {
		t.Fatalf("Range = %q", part)
	}

	// Listing under a prefix, with paging.
	prefix := "lake/default/sales/version=1/attempt=1/"
	var all []string
	token := ""
	for pages := 0; pages < 10; pages++ {
		res, err := c.List(ctx, ListOptions{Prefix: prefix, MaxKeys: 2, Token: token})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		for _, o := range res.Objects {
			all = append(all, o.Key)
			if o.Size != int64(len(payload)) || o.LastModified.IsZero() {
				t.Fatalf("object metadata %+v", o)
			}
		}
		if !res.Truncated {
			break
		}
		token = res.NextToken
	}
	if len(all) != 4 {
		t.Fatalf("listed %d keys under %s: %v", len(all), prefix, all)
	}

	// Delete is idempotent and removes the object.
	if err := c.Delete(ctx, keys[0]); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, keys[0]); err != nil {
		t.Fatalf("deleting a missing key must not fail: %v", err)
	}
	if _, err := c.Get(ctx, keys[0], 1<<20); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	for _, k := range keys[1:] {
		_ = c.Delete(ctx, k)
	}
}

func TestS3RealStoreRejectsBadCredentials(t *testing.T) {
	ctx := context.Background()
	good := s3Client(t, "")
	if err := good.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	bad := New(Config{Endpoint: os.Getenv("TEST_S3_ENDPOINT"), AccessKey: os.Getenv("TEST_S3_ACCESS_KEY"),
		SecretKey: "wrong-secret-wrong-secret", Bucket: good.Bucket(), Region: "us-east-1"})
	if err := bad.Put(ctx, "x", []byte("y")); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("Put with a wrong secret: %v", err)
	}
	if _, err := bad.Get(ctx, "x", 10); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("Get with a wrong secret: %v", err)
	}
}
