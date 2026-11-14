package objectstore

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

// emptyPayloadHash is the SHA-256 of an empty body, used for GET requests.
const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func sha256HexString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}

func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

// pctEncode percent-encodes s per RFC 3986: it leaves [A-Za-z0-9-._~] intact
// and encodes every other byte as an uppercase %HH sequence. This is the
// encoding used for S3 canonical URIs and query strings.
func pctEncode(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isUnreserved(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0x0f])
	}
	return b.String()
}

func isUnreserved(c byte) bool {
	return (c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') ||
		c == '-' || c == '.' || c == '_' || c == '~'
}

// encodeKeyPath returns the encoded object-key path component: each segment is
// percent-encoded and "/" separators are preserved.
func encodeKeyPath(key string) string {
	if key == "" {
		return ""
	}
	segs := strings.Split(key, "/")
	for i := range segs {
		segs[i] = pctEncode(segs[i])
	}
	return strings.Join(segs, "/")
}

// encodeQuery encodes query parameters in sorted key order. For S3 the request
// query string and the SigV4 canonical query string are the same single-encoded
// form.
func encodeQuery(params [][2]string) string {
	sorted := make([][2]string, len(params))
	copy(sorted, params)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i][0] != sorted[j][0] {
			return sorted[i][0] < sorted[j][0]
		}
		return sorted[i][1] < sorted[j][1]
	})
	parts := make([]string, 0, len(sorted))
	for _, p := range sorted {
		parts = append(parts, pctEncode(p[0])+"="+pctEncode(p[1]))
	}
	return strings.Join(parts, "&")
}

// sign computes the AWS Signature Version 4 headers for a request and returns
// the headers that must be set. host is the endpoint (host:port); canonicalURI
// and canonicalQuery are the already-encoded forms.
func sign(accessKey, secretKey, region, host, method, canonicalURI, canonicalQuery, payloadHash string, now time.Time) map[string]string {
	if region == "" {
		region = "us-east-1"
	}
	amzDate := now.UTC().Format("20060102T150405Z")
	date := now.UTC().Format("20060102")

	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := date + "/" + region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256HexString(canonicalRequest),
	}, "\n")

	signingKey := deriveSigningKey(secretKey, date, region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	return map[string]string{
		"Authorization":        "AWS4-HMAC-SHA256 Credential=" + accessKey + "/" + scope + ", SignedHeaders=" + signedHeaders + ", Signature=" + signature,
		"X-Amz-Date":           amzDate,
		"X-Amz-Content-Sha256": payloadHash,
	}
}
