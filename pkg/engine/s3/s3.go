// Package s3 provides an S3-compatible storage engine.
//
// The engine uses aws-sdk-go-v2 with endpoint override support, so the same
// implementation covers AWS S3, MinIO, Ceph RGW, and S3-compatible gateways.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/chanuollala/ioflux/pkg/engine"
)

const (
	defaultRegion             = "us-east-1"
	defaultMultipartThreshold = int64(64 << 20)
	defaultMultipartPartSize  = int64(16 << 20)
	minMultipartPartSize      = int64(5 << 20)
)

// Config configures S3Engine.
type Config struct {
	Endpoint string
	Region   string
	Bucket   string

	PathStyle bool

	AccessKey    string
	SecretKey    string
	SessionToken string

	MultipartThreshold int64
	MultipartPartSize  int64

	// DisableHTTPKeepAlive approximates cold object-store replay by preventing
	// client-side HTTP connection reuse.
	DisableHTTPKeepAlive bool

	// HeadOnOpen verifies an object exists when a file-shaped trace OPENs it.
	// Default false keeps OPEN cheap; READ reports missing objects.
	HeadOnOpen bool
}

// S3Engine is an Engine backed by S3-compatible object storage.
type S3Engine struct {
	client *awss3.Client
	cfg    Config

	mu      sync.Mutex
	handles map[engine.Handle]string
	nextH   atomic.Int64
}

// New creates an S3Engine from cfg.
func New(cfg Config) (*S3Engine, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3: bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = defaultRegion
	}
	if cfg.MultipartThreshold <= 0 {
		cfg.MultipartThreshold = defaultMultipartThreshold
	}
	if cfg.MultipartPartSize <= 0 {
		cfg.MultipartPartSize = defaultMultipartPartSize
	}
	if cfg.MultipartPartSize < minMultipartPartSize {
		return nil, fmt.Errorf("s3: multipart part size %d is below S3 minimum %d", cfg.MultipartPartSize, minMultipartPartSize)
	}
	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return nil, fmt.Errorf("s3: access key and secret key must be provided together")
	}

	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}
	if cfg.Endpoint != "" {
		loadOpts = append(loadOpts, config.WithBaseEndpoint(cfg.Endpoint))
	}
	if cfg.AccessKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken),
		))
	}
	if cfg.DisableHTTPKeepAlive {
		loadOpts = append(loadOpts, config.WithHTTPClient(coldHTTPClient()))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3: load config: %w", err)
	}

	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.UsePathStyle = cfg.PathStyle
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})

	return &S3Engine{
		client:  client,
		cfg:     cfg,
		handles: make(map[engine.Handle]string),
	}, nil
}

func coldHTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableKeepAlives = true
	tr.MaxIdleConns = 0
	tr.MaxIdleConnsPerHost = 0
	return &http.Client{Transport: tr}
}

func addUnsignedPayloadMiddleware(stack *middleware.Stack) error {
	v4.RemoveContentSHA256HeaderMiddleware(stack)
	v4.RemoveComputePayloadSHA256Middleware(stack)
	return v4.AddUnsignedPayloadMiddleware(stack)
}

// Caps returns S3 capabilities. Seekable means range GETs are supported for
// offset reads; PartialWrite is false because S3 cannot perform arbitrary
// offset writes.
func (e *S3Engine) Caps() engine.Capabilities {
	return engine.Capabilities{
		Seekable:     true,
		PartialWrite: false,
		Durable:      false,
		ObjectAPI:    true,
		Multipart:    true,
		OSPageCache:  false,
	}
}

// Open binds a trace handle to an object key. It performs no network call unless
// Config.HeadOnOpen is set.
func (e *S3Engine) Open(ctx context.Context, target string, mode engine.Mode, _ engine.OpenFlags) (engine.Handle, error) {
	if mode != engine.ModeRead {
		return 0, fmt.Errorf("s3: open %q with mode %q: %w", target, mode, engine.ErrUnsupported)
	}
	if e.cfg.HeadOnOpen {
		if _, err := e.Head(ctx, target); err != nil {
			return 0, err
		}
	}
	h := engine.Handle(e.nextH.Add(1))
	e.mu.Lock()
	e.handles[h] = target
	e.mu.Unlock()
	return h, nil
}

// Read issues a Range GET against the key bound to h.
func (e *S3Engine) Read(ctx context.Context, h engine.Handle, off, length int64, buf []byte) (int, error) {
	key, err := e.lookupHandle(h)
	if err != nil {
		return 0, err
	}
	return e.Get(ctx, key, off, length, buf)
}

// Write is intentionally unsupported in M1; offset writes are rejected at
// PREPARE via Caps().PartialWrite=false.
func (e *S3Engine) Write(_ context.Context, _ engine.Handle, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}

// Fsync is not meaningful for S3.
func (e *S3Engine) Fsync(_ context.Context, _ engine.Handle) error {
	return engine.ErrUnsupported
}

// Close drops the local handle. It performs no network call.
func (e *S3Engine) Close(_ context.Context, h engine.Handle) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.handles[h]; !ok {
		return fmt.Errorf("s3: close: unknown handle %d: %w", h, engine.ErrNotFound)
	}
	delete(e.handles, h)
	return nil
}

// Stat delegates to Head so file-shaped traces can use STAT against object
// targets after target mapping.
func (e *S3Engine) Stat(ctx context.Context, target string) (engine.ObjectInfo, error) {
	return e.Head(ctx, target)
}

// Put writes a whole object. Large objects use multipart upload.
func (e *S3Engine) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if size < 0 {
		return fmt.Errorf("s3: put %q: negative size %d", key, size)
	}
	if size > e.cfg.MultipartThreshold {
		return e.putMultipart(ctx, key, r, size)
	}
	_, err := e.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(e.cfg.Bucket),
		Key:           aws.String(key),
		Body:          r,
		ContentLength: aws.Int64(size),
	}, awss3.WithAPIOptions(addUnsignedPayloadMiddleware))
	return mapErr("put "+key, err)
}

// Get reads length bytes from key at off using an S3 Range GET.
func (e *S3Engine) Get(ctx context.Context, key string, off, length int64, buf []byte) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("s3: get %q: offset %d must be non-negative", key, off)
	}
	if length < 0 {
		return 0, fmt.Errorf("s3: get %q: length %d must be non-negative", key, length)
	}
	if length == 0 {
		return 0, nil
	}
	if int64(len(buf)) < length {
		return 0, fmt.Errorf("s3: get %q: buffer length %d < requested length %d", key, len(buf), length)
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, off+length-1)
	out, err := e.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(e.cfg.Bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return 0, mapErr("get "+key, err)
	}
	defer out.Body.Close()

	n, readErr := io.ReadFull(out.Body, buf[:length])
	if readErr == nil {
		return n, nil
	}
	if errors.Is(readErr, io.ErrUnexpectedEOF) || errors.Is(readErr, io.EOF) {
		return n, engine.ErrShortRead
	}
	return n, fmt.Errorf("s3: get %q: read response body: %w", key, readErr)
}

// Head returns object metadata.
func (e *S3Engine) Head(ctx context.Context, key string) (engine.ObjectInfo, error) {
	out, err := e.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(e.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return engine.ObjectInfo{}, mapErr("head "+key, err)
	}
	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return engine.ObjectInfo{Name: key, Size: size}, nil
}

// Delete removes key.
func (e *S3Engine) Delete(ctx context.Context, key string) error {
	_, err := e.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(e.cfg.Bucket),
		Key:    aws.String(key),
	})
	return mapErr("delete "+key, err)
}

func (e *S3Engine) lookupHandle(h engine.Handle) (string, error) {
	e.mu.Lock()
	key, ok := e.handles[h]
	e.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("s3: unknown handle %d: %w", h, engine.ErrNotFound)
	}
	return key, nil
}

func mapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound {
		return fmt.Errorf("s3: %s: %w", op, engine.ErrNotFound)
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		code := ae.ErrorCode()
		if code == "NoSuchKey" || code == "NotFound" || code == "NoSuchBucket" {
			return fmt.Errorf("s3: %s: %w", op, engine.ErrNotFound)
		}
	}
	if strings.Contains(err.Error(), "status code: 404") {
		return fmt.Errorf("s3: %s: %w", op, engine.ErrNotFound)
	}
	return fmt.Errorf("s3: %s: %w", op, err)
}
