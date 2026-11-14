package objectstore

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// maxListKeys caps a single object-store listing page. Registrations and
// previews list at most this many keys per page, so a prefix with many objects
// is never enumerated without bound.
const maxListKeys = 1000

// maxResponseBytes bounds the size of an XML listing response the client will
// read.
const maxResponseBytes = 16 << 20

// Config configures the MinIO/S3 adapter. Credentials are server-side only.
type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Region    string
	UseSSL    bool
}

// Client is a MinIO/S3-compatible object-store adapter using SigV4.
type Client struct {
	cfg  Config
	http *http.Client
}

// New builds a Client.
func New(cfg Config) *Client {
	return &Client{
		cfg: cfg,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Bucket returns the configured bucket name.
func (c *Client) Bucket() string { return c.cfg.Bucket }

func (c *Client) scheme() string {
	if c.cfg.UseSSL {
		return "https"
	}
	return "http"
}

// path returns the encoded request path for the given object key, including the
// leading bucket segment.
func (c *Client) path(key string) string {
	p := "/" + pctEncode(c.cfg.Bucket)
	if key != "" {
		p += "/" + encodeKeyPath(key)
	}
	return p
}

// List lists objects under a prefix with a bounded page size.
func (c *Client) List(ctx context.Context, opts ListOptions) (ListResult, error) {
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 || maxKeys > maxListKeys {
		maxKeys = maxListKeys
	}

	params := [][2]string{
		{"list-type", "2"},
		{"max-keys", strconv.Itoa(maxKeys)},
	}
	if opts.Prefix != "" {
		params = append(params, [2]string{"prefix", opts.Prefix})
	}
	if opts.Token != "" {
		params = append(params, [2]string{"continuation-token", opts.Token})
	}

	query := encodeQuery(params)
	rawURL := c.scheme() + "://" + c.cfg.Endpoint + c.path("") + "?" + query

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return ListResult{}, err
	}
	c.sign(req, http.MethodGet, c.path(""), query, emptyPayloadHash)

	resp, err := c.http.Do(req)
	if err != nil {
		return ListResult{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return ListResult{}, ErrNotFound
	case http.StatusForbidden:
		return ListResult{}, ErrAccessDenied
	default:
		return ListResult{}, fmt.Errorf("%w: list returned status %d", ErrUnavailable, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return ListResult{}, fmt.Errorf("%w: read list response: %v", ErrUnavailable, err)
	}

	var parsed listBucketResult
	if err := xml.Unmarshal(body, &parsed); err != nil {
		return ListResult{}, fmt.Errorf("%w: parse list response: %v", ErrUnavailable, err)
	}

	out := ListResult{
		NextToken: parsed.NextContinuationToken,
		Truncated: parsed.IsTruncated,
		Objects:   make([]Object, 0, len(parsed.Contents)),
	}
	for _, c := range parsed.Contents {
		obj := Object{
			Key:  c.Key,
			Size: c.Size,
		}
		if c.LastModified != "" {
			if t, err := time.Parse(time.RFC3339, c.LastModified); err == nil {
				obj.LastModified = t
			}
		}
		out.Objects = append(out.Objects, obj)
	}
	return out, nil
}

// Range returns a bounded read of [offset, offset+length) of the object at key.
func (c *Client) Range(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	rawURL := c.scheme() + "://" + c.cfg.Endpoint + c.path(key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	if length > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}
	c.sign(req, http.MethodGet, c.path(key), "", emptyPayloadHash)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return resp.Body, nil
	case http.StatusNotFound:
		_ = resp.Body.Close()
		return nil, ErrNotFound
	case http.StatusForbidden:
		_ = resp.Body.Close()
		return nil, ErrAccessDenied
	default:
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: range returned status %d", ErrUnavailable, resp.StatusCode)
	}
}

func (c *Client) sign(req *http.Request, method, canonicalURI, canonicalQuery, payloadHash string) {
	headers := sign(c.cfg.AccessKey, c.cfg.SecretKey, c.cfg.Region, c.cfg.Endpoint, method, canonicalURI, canonicalQuery, payloadHash, time.Now())
	for k, v := range headers {
		req.Header.Set(k, v)
	}
}

// listBucketResult mirrors the ListObjectsV2 XML response.
type listBucketResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	Name                  string   `xml:"Name"`
	Prefix                string   `xml:"Prefix"`
	KeyCount              int      `xml:"KeyCount"`
	MaxKeys               int      `xml:"MaxKeys"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
	Contents              []struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
	} `xml:"Contents"`
}
