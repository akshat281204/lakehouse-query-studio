package objectstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPctEncode(t *testing.T) {
	cases := map[string]string{
		"abc-123._~": "abc-123._~",
		"a b":        "a%20b",
		"a=b":        "a%3Db",
		"a/b":        "a%2Fb",
		"a+b":        "a%2Bb",
		"café":       "caf%C3%A9",
	}
	for in, want := range cases {
		if got := pctEncode(in); got != want {
			t.Errorf("pctEncode(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEncodeQuerySorted(t *testing.T) {
	got := encodeQuery([][2]string{
		{"max-keys", "1000"},
		{"prefix", "data/sales"},
		{"list-type", "2"},
	})
	want := "list-type=2&max-keys=1000&prefix=data%2Fsales"
	if got != want {
		t.Errorf("encodeQuery() = %q, want %q", got, want)
	}
}

func TestSignDeterministic(t *testing.T) {
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	a := sign("AKID", "secret", "us-east-1", "minio:9000", "GET", "/bucket/data%2Fsales", "list-type=2", emptyPayloadHash, now)
	b := sign("AKID", "secret", "us-east-1", "minio:9000", "GET", "/bucket/data%2Fsales", "list-type=2", emptyPayloadHash, now)

	if a["Authorization"] != b["Authorization"] {
		t.Fatal("sign() is not deterministic for a fixed timestamp")
	}
	if !strings.HasPrefix(a["Authorization"], "AWS4-HMAC-SHA256 Credential=AKID/") {
		t.Errorf("Authorization header malformed: %q", a["Authorization"])
	}
	if a["X-Amz-Content-Sha256"] != emptyPayloadHash {
		t.Errorf("payload hash header = %q", a["X-Amz-Content-Sha256"])
	}
	if a["X-Amz-Date"] != "20240102T030405Z" {
		t.Errorf("amz date = %q", a["X-Amz-Date"])
	}
}

func TestClientList(t *testing.T) {
	const xmlBody = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>bucket</Name>
  <Prefix>data/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>data/a.parquet</Key>
    <LastModified>2024-01-02T03:04:05.000Z</LastModified>
    <ETag>"etag-a"</ETag>
    <Size>10</Size>
  </Contents>
  <Contents>
    <Key>data/b.parquet</Key>
    <LastModified>2024-01-02T03:05:00.000Z</LastModified>
    <ETag>"etag-b"</ETag>
    <Size>20</Size>
  </Contents>
</ListBucketResult>`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "2" {
			t.Errorf("missing list-type=2")
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, xmlBody)
	}))
	defer srv.Close()

	client := New(Config{
		Endpoint:  strings.TrimPrefix(srv.URL, "http://"),
		AccessKey: "AKID",
		SecretKey: "secret",
		Bucket:    "bucket",
		Region:    "us-east-1",
	})

	res, err := client.List(context.Background(), ListOptions{Prefix: "data/", MaxKeys: 1000})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(res.Objects) != 2 {
		t.Fatalf("List() returned %d objects, want 2", len(res.Objects))
	}
	if res.Objects[0].Key != "data/a.parquet" || res.Objects[0].Size != 10 {
		t.Errorf("first object = %+v", res.Objects[0])
	}
	if res.Truncated {
		t.Error("Truncated = true, want false")
	}
}

func TestClientListNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	client := New(Config{Endpoint: strings.TrimPrefix(srv.URL, "http://"), Bucket: "bucket"})
	if _, err := client.List(context.Background(), ListOptions{Prefix: "data/"}); err != ErrNotFound {
		t.Fatalf("List() error = %v, want ErrNotFound", err)
	}
}

func TestClientRange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=0-7" {
			t.Errorf("Range header = %q, want bytes=0-7", got)
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "PAR1abcd")
	}))
	defer srv.Close()

	client := New(Config{Endpoint: strings.TrimPrefix(srv.URL, "http://"), Bucket: "bucket"})
	rc, err := client.Range(context.Background(), "data/a.parquet", 0, 8)
	if err != nil {
		t.Fatalf("Range() error = %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if string(got) != "PAR1abcd" {
		t.Errorf("range body = %q", got)
	}
}

func TestClientRangeNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	client := New(Config{Endpoint: strings.TrimPrefix(srv.URL, "http://"), Bucket: "bucket"})
	if _, err := client.Range(context.Background(), "missing", 0, 8); err != ErrNotFound {
		t.Fatalf("Range() error = %v, want ErrNotFound", err)
	}
}
