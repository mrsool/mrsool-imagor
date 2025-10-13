package s3storage

import (
	"context"
	"net/http"
	"strings"
)

// S3ConfigMiddleware is a middleware that extracts the AWS-BUCKET header
// and adds it to the request context for use by S3Storage
func S3ConfigMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := r.Header.Get("AWS-BUCKET")
		region := r.Header.Get("AWS-REGION")
		if bucket != "" {
			bucket = strings.TrimSpace(bucket)
			ctx := context.WithValue(r.Context(), "aws-bucket", bucket)
			r = r.WithContext(ctx)
		}
		if region != "" {
			region = strings.TrimSpace(region)
			ctx := context.WithValue(r.Context(), "aws-region", region)
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}
