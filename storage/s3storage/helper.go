package s3storage

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
)

// getBucketFromRequest extracts the bucket name from the "AWS-BUCKET" header
func (s *S3Storage) getBucketFromRequest(r *http.Request) string {
	bucket := r.Header.Get("AWS-BUCKET")
	if bucket == "" {
		return s.Bucket
	}
	return bucket
}

// getBucketFromContext extracts the bucket name from the context
func (s *S3Storage) getBucketFromContext(ctx context.Context) string {
	if bucket, ok := ctx.Value("aws-bucket").(string); ok && bucket != "" {
		return bucket
	}
	return s.Bucket
}

// getRegionFromRequest extracts the AWS region from the "AWS-REGION" header
func (s *S3Storage) getRegionFromRequest(r *http.Request) string {
	region := r.Header.Get("AWS-REGION")
	if region == "" {
		return ""
	}
	s.Logger.Info("AWS-REGION", zap.String("region", region))
	return region
}

// getRegionFromContext extracts the AWS region from the context
func (s *S3Storage) getRegionFromContext(ctx context.Context) string {
	if region, ok := ctx.Value("aws-region").(string); ok && region != "" {
		s.Logger.Info("AWS-REGION", zap.String("region", region))
		return region
	}
	return ""
}

// getClientWithRegion creates an S3 client with the specified region
func (s *S3Storage) getClientWithRegion(region string) *s3.Client {
	if region == "" {
		// Use default client if no region override
		return s.Client
	}

	// Create a new client with the specified region
	cfg := s.baseConfig
	cfg.Region = region

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

	return s3.NewFromConfig(cfg, s3Options...)
}
