package prometheusmetrics

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "http_request_duration_seconds",
			Help: "A histogram of latencies for requests",
		},
		[]string{"code", "method", "source"},
	)
)

// PrometheusMetrics wraps the Service with additional http and app lifecycle handling
type PrometheusMetrics struct {
	http.Server

	Path   string
	Logger *zap.Logger
}

// New create new metrics PrometheusMetrics
func New(options ...Option) *PrometheusMetrics {
	s := &PrometheusMetrics{
		Path:   "/",
		Logger: zap.NewNop(),
	}
	for _, option := range options {
		option(s)
	}
	if s.Path != "" && s.Path != "/" {
		mux := http.NewServeMux()
		mux.Handle(s.Path, promhttp.Handler())
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, s.Path, http.StatusPermanentRedirect)
		})
		s.Handler = mux
	} else {
		s.Handler = promhttp.Handler()
	}
	return s
}

// Startup prometheus metrics server
func (s *PrometheusMetrics) Startup(_ context.Context) error {
	if err := prometheus.Register(httpRequestDuration); err != nil {
		return err
	}

	go func() {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.Logger.Fatal("prometheus listen", zap.Error(err))
		}
	}()
	s.Logger.Info("prometheus listen", zap.String("addr", s.Addr), zap.String("path", s.Path))
	return nil
}

// Handle prometheus http middleware handler
func (s *PrometheusMetrics) Handle(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract source from AWS-BUCKET header
		source := r.Header.Get("AWS-BUCKET")
		if source == "" {
			source = "unknown"
		}

		// Create a custom response writer to capture status code
		recorder := &responseRecorder{
			ResponseWriter: w,
			statusCode:     200,
		}

		// Record start time
		start := time.Now()

		// Call the next handler
		next.ServeHTTP(recorder, r)

		// Calculate duration
		duration := time.Since(start)

		// Record the metric with source label
		httpRequestDuration.WithLabelValues(
			strconv.Itoa(recorder.statusCode),
			r.Method,
			source,
		).Observe(duration.Seconds())
	})
}

// Option PrometheusMetrics option
type Option func(s *PrometheusMetrics)

// WithAddr with server and port option
func WithAddr(addr string) Option {
	return func(s *PrometheusMetrics) {
		s.Addr = addr
	}
}

// WithPath with path option
func WithPath(path string) Option {
	return func(s *PrometheusMetrics) {
		s.Path = path
	}
}

// WithLogger with logger option
func WithLogger(logger *zap.Logger) Option {
	return func(s *PrometheusMetrics) {
		if logger != nil {
			s.Logger = logger
		}
	}
}

// responseRecorder captures the HTTP status code for metrics
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}
