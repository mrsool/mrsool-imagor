package s3storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/cshum/imagor"
	"github.com/cshum/imagor/imagorpath"
	"go.uber.org/zap"
)

// S3Storage AWS S3 Storage implements imagor.Storage interface
type S3Storage struct {
	Client *s3.Client
	Bucket string

	BaseDir        string
	PathPrefix     string
	ACL            string
	SafeChars      string
	StorageClass   string
	Expiration     time.Duration
	Endpoint       string
	ForcePathStyle bool
	Logger         *zap.Logger

	// Imgix fallback configuration
	ImgixFallbackEnabled bool
	ImgixFallbackTimeout time.Duration

	safeChars         imagorpath.SafeChars
	bucketFromRequest bool
	regionFromRequest bool
	baseConfig        aws.Config
}

// New creates S3Storage
func New(cfg aws.Config, bucket string, options ...Option) *S3Storage {
	baseDir := "/"
	if idx := strings.Index(bucket, "/"); idx > -1 {
		baseDir = bucket[idx:]
		bucket = bucket[:idx]
	}
	s := &S3Storage{
		Bucket: bucket,

		BaseDir:    baseDir,
		PathPrefix: "/",
		ACL:        "", // Default to no ACL to avoid ACL errors on modern buckets
		Logger:     zap.NewNop(),
		baseConfig: cfg,
	}
	for _, option := range options {
		option(s)
	}

	// Create S3 client with endpoint and path style options
	var s3Options []func(*s3.Options)
	if s.Endpoint != "" {
		s3Options = append(s3Options, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(s.Endpoint)
			o.DisableLogOutputChecksumValidationSkipped = true
		})
	}
	if s.ForcePathStyle {
		s3Options = append(s3Options, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
	s.Client = s3.NewFromConfig(cfg, s3Options...)

	if s.SafeChars == "--" {
		s.safeChars = imagorpath.NewNoopSafeChars()
	} else {
		s.safeChars = imagorpath.NewSafeChars("!\"()*" + s.SafeChars)
		// https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-guidelines-safe-characters
	}

	return s
}

// Path transforms and validates image key for storage path
func (s *S3Storage) Path(image string) (string, bool) {
	image = "/" + imagorpath.Normalize(image, s.safeChars)
	if !strings.HasPrefix(image, s.PathPrefix) {
		return "", false
	}
	result := filepath.Join(s.BaseDir, strings.TrimPrefix(image, s.PathPrefix))
	if len(result) > 0 && result[0] == '/' {
		result = result[1:]
	}
	return result, true
}

// Get implements imagor.Storage interface
func (s *S3Storage) Get(r *http.Request, image string) (*imagor.Blob, error) {
	ctx := r.Context()
	image, ok := s.Path(image)
	if !ok {
		return nil, imagor.ErrInvalid
	}
	bucket := s.getBucketFromRequest(r)
	region := s.getRegionFromRequest(r)
	client := s.getClientWithRegion(region)

	// Get bucket prefix for key when bucketFromRequest is false
	bucketPrefix := s.getBucketFromRequestForKey(r)
	key := s.buildKeyWithBucketPrefix(image, bucketPrefix)

	var blob *imagor.Blob
	var once sync.Once
	blob = imagor.NewBlob(func() (io.ReadCloser, int64, error) {
		input := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}
		out, err := client.GetObject(ctx, input)
		if err != nil {
			s.Logger.Info("S3 get object error",
				zap.String("bucket", bucket),
				zap.String("key", key),
				zap.String("region", region),
				zap.String("original_image", r.URL.Path))

			// If it's a not found error and imgix fallback is enabled, try imgix
			if isNotFoundError(err) && s.ImgixFallbackEnabled {
				return s.fetchFromImgixAndSave(ctx, r, bucket, key, image)
			}

			if isNotFoundError(err) {
				return nil, 0, imagor.ErrNotFound
			}
			return nil, 0, err
		}
		once.Do(func() {
			if out.ContentType != nil {
				blob.SetContentType(*out.ContentType)
			}
			if out.ContentLength != nil && out.ETag != nil && out.LastModified != nil {
				blob.Stat = &imagor.Stat{
					Size:         *out.ContentLength,
					ETag:         *out.ETag,
					ModifiedTime: *out.LastModified,
				}
			}
		})
		if s.Expiration > 0 && out.LastModified != nil {
			if time.Since(*out.LastModified) > s.Expiration {
				return nil, 0, imagor.ErrExpired
			}
		}
		var size int64
		if out.ContentLength != nil {
			size = *out.ContentLength
		}
		return out.Body, size, nil
	})
	return blob, nil
}

// Put implements imagor.Storage interface
func (s *S3Storage) Put(ctx context.Context, image string, blob *imagor.Blob) error {
	image, ok := s.Path(image)
	if !ok {
		return imagor.ErrInvalid
	}
	bucket := s.getBucketFromContext(ctx)
	region := s.getRegionFromContext(ctx)
	client := s.getClientWithRegion(region)

	// Get bucket prefix for key when bucketFromRequest is false
	bucketPrefix := s.getBucketFromContextForKey(ctx)
	key := s.buildKeyWithBucketPrefix(image, bucketPrefix)

	reader, size, err := blob.NewReader()
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.Close()
	}()
	input := &s3.PutObjectInput{
		Body:          reader,
		Bucket:        aws.String(bucket),
		ContentType:   aws.String(blob.ContentType()),
		ContentLength: aws.Int64(size),
		Key:           aws.String(key),
		StorageClass:  types.StorageClass(s.StorageClass),
	}

	// Only set ACL if it's explicitly configured
	// This prevents ACL errors on buckets that don't support ACLs
	if s.ACL != "" {
		input.ACL = types.ObjectCannedACL(s.ACL)
	}
	_, err = client.PutObject(ctx, input)
	if err != nil {
		s.Logger.Info("S3 put object error",
			zap.String("bucket", bucket),
			zap.String("key", key),
			zap.String("region", region),
			zap.String("original_image", image))
	}
	return err
}

// Delete implements imagor.Storage interface
func (s *S3Storage) Delete(ctx context.Context, image string) error {
	image, ok := s.Path(image)
	if !ok {
		return imagor.ErrInvalid
	}
	bucket := s.getBucketFromContext(ctx)
	region := s.getRegionFromContext(ctx)
	client := s.getClientWithRegion(region)

	// Get bucket prefix for key when bucketFromRequest is false
	bucketPrefix := s.getBucketFromContextForKey(ctx)
	key := s.buildKeyWithBucketPrefix(image, bucketPrefix)

	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		s.Logger.Info("S3 delete object error",
			zap.String("bucket", bucket),
			zap.String("key", key),
			zap.String("region", region),
			zap.String("original_image", image))
	}
	return err
}

// Stat implements imagor.Storage interface
func (s *S3Storage) Stat(ctx context.Context, image string) (stat *imagor.Stat, err error) {
	image, ok := s.Path(image)
	if !ok {
		return nil, imagor.ErrInvalid
	}
	bucket := s.getBucketFromContext(ctx)
	region := s.getRegionFromContext(ctx)
	client := s.getClientWithRegion(region)

	// Get bucket prefix for key when bucketFromRequest is false
	bucketPrefix := s.getBucketFromContextForKey(ctx)
	key := s.buildKeyWithBucketPrefix(image, bucketPrefix)

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	head, err := client.HeadObject(ctx, input)
	if err != nil {
		if isNotFoundError(err) {
			return nil, imagor.ErrNotFound
		}
		s.Logger.Info("S3 stat error",
			zap.String("bucket", bucket),
			zap.String("key", key))
		return nil, err
	}
	return &imagor.Stat{
		Size:         *head.ContentLength,
		ETag:         *head.ETag,
		ModifiedTime: *head.LastModified,
	}, nil
}

// fetchFromImgixAndSave fetches the image from imgix and saves it to S3
func (s *S3Storage) fetchFromImgixAndSave(ctx context.Context, r *http.Request, bucket, key, image string) (io.ReadCloser, int64, error) {
	imgixURL := fmt.Sprintf("https://%s.imgix.net/%s?q=100&w=1", bucket, image)

	s.Logger.Info("Attempting imgix fallback",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("imgix_url", imgixURL),
		zap.String("original_image", r.URL.Path))

	// Create HTTP client with timeout
	timeout := s.ImgixFallbackTimeout
	if timeout == 0 {
		timeout = 30 * time.Second // Default timeout - increased for imgix
	}

	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequestWithContext(ctx, "GET", imgixURL, nil)
	if err != nil {
		s.Logger.Error("Failed to create imgix request", zap.Error(err))
		return nil, 0, imagor.ErrNotFound
	}

	resp, err := client.Do(req)
	if err != nil {
		s.Logger.Error("Failed to fetch from imgix",
			zap.Error(err),
			zap.String("url", imgixURL),
			zap.Duration("timeout", timeout))
		return nil, 0, imagor.ErrNotFound
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		s.Logger.Error("Imgix returned non-200 status",
			zap.Int("status", resp.StatusCode),
			zap.String("url", imgixURL))
		return nil, 0, imagor.ErrNotFound
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.Logger.Error("Failed to read imgix response", zap.Error(err))
		return nil, 0, imagor.ErrNotFound
	}

	// Save to S3
	putInput := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          strings.NewReader(string(body)),
		ContentType:   aws.String(resp.Header.Get("Content-Type")),
		ContentLength: aws.Int64(int64(len(body))),
		StorageClass:  types.StorageClass(s.StorageClass),
	}

	// Only set ACL if it's explicitly configured
	if s.ACL != "" {
		putInput.ACL = types.ObjectCannedACL(s.ACL)
	}

	_, err = s.getClientWithRegion(s.getRegionFromRequest(r)).PutObject(ctx, putInput)
	if err != nil {
		s.Logger.Error("Failed to save imgix content to S3", zap.Error(err))
		// Still return the content even if saving failed
		return io.NopCloser(strings.NewReader(string(body))), int64(len(body)), nil
	}

	s.Logger.Info("Successfully fetched from imgix and saved to S3",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("imgix_url", imgixURL))

	// Return the content
	return io.NopCloser(strings.NewReader(string(body))), int64(len(body)), nil
}

// Helper function for not found errors
func isNotFoundError(err error) bool {
	var nsk *types.NoSuchKey
	var nbf *types.NoSuchBucket
	if errors.As(err, &nsk) || errors.As(err, &nbf) {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}
