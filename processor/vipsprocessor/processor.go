package vipsprocessor

import (
	"context"
	"math"
	"runtime"
	"strings"
	"sync"

	"github.com/cshum/imagor"
	"github.com/cshum/imagor/metrics/instrumentation"
	"github.com/cshum/vipsgen/vips"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
)

// FilterFunc filter handler function
type FilterFunc func(ctx context.Context, img *vips.Image, load imagor.LoadFunc, args ...string) (err error)

// FilterMap filter handler map
type FilterMap map[string]FilterFunc

var processorLock sync.RWMutex
var processorCount int

// Global context storage for VIPS logging
var (
	currentContext context.Context
	contextMutex   sync.RWMutex
)

// setCurrentContext sets the current context for VIPS logging
func setCurrentContext(ctx context.Context) {
	contextMutex.Lock()
	currentContext = ctx
	contextMutex.Unlock()
}

// getCurrentContext gets the current context for VIPS logging
func getCurrentContext() context.Context {
	contextMutex.RLock()
	defer contextMutex.RUnlock()
	return currentContext
}

// withContextLogger creates a logger that automatically includes request ID from context
func (v *Processor) withContextLogger(ctx context.Context) *zap.Logger {
	if requestID := imagor.GetRequestID(ctx); requestID != "" {
		return v.Logger.With(zap.String("request_id", requestID))
	}
	return v.Logger
}

// NewMethodTimer creates a new method timer for instrumentation
func (v *Processor) NewMethodTimer(identifier string) *instrumentation.MethodTimer {
	if v.Instrumentation == nil {
		return nil
	}
	return v.Instrumentation.NewMethodTimerFromString(identifier)
}

// Processor implements imagor.Processor interface
type Processor struct {
	Filters            FilterMap
	FallbackFunc       FallbackFunc
	DisableBlur        bool
	DisableFilters     []string
	MaxFilterOps       int
	Logger             *zap.Logger
	Concurrency        int
	MaxCacheFiles      int
	MaxCacheMem        int
	MaxCacheSize       int
	MaxWidth           int
	MaxHeight          int
	MaxResolution      int
	MaxAnimationFrames int
	MozJPEG            bool
	StripMetadata      bool
	AvifSpeed          int
	Unlimited          bool
	Debug              bool
	PNGBufferThreshold int64 // Threshold for loading PNG files into buffer to avoid streaming issues
	Instrumentation    *instrumentation.Instrumentation

	disableFilters map[string]bool
}

// NewProcessor create Processor
func NewProcessor(options ...Option) *Processor {
	v := &Processor{
		MaxWidth:           9999,
		MaxHeight:          9999,
		MaxResolution:      81000000,
		Concurrency:        1,
		MaxFilterOps:       -1,
		MaxAnimationFrames: -1,
		PNGBufferThreshold: 1024 * 1024, // 1MB default threshold for large PNGs
		Logger:             zap.NewNop(),
		disableFilters:     map[string]bool{},
	}
	v.Filters = FilterMap{
		"watermark":        v.watermark,
		"round_corner":     roundCorner,
		"rotate":           rotate,
		"label":            label,
		"grayscale":        grayscale,
		"brightness":       brightness,
		"background_color": backgroundColor,
		"contrast":         contrast,
		"modulate":         modulate,
		"hue":              hue,
		"saturation":       saturation,
		"rgb":              rgb,
		"blur":             blur,
		"sharpen":          sharpen,
		"strip_icc":        stripIcc,
		"strip_exif":       stripExif,
		"trim":             trim,
		"padding":          v.padding,
		"proportion":       proportion,
	}
	for _, option := range options {
		option(v)
	}
	if v.DisableBlur {
		v.DisableFilters = append(v.DisableFilters, "blur", "sharpen")
	}
	for _, name := range v.DisableFilters {
		v.disableFilters[name] = true
	}
	if v.Concurrency == -1 {
		v.Concurrency = runtime.NumCPU()
	}
	return v
}

// Startup implements imagor.Processor interface
func (v *Processor) Startup(_ context.Context) error {
	processorLock.Lock()
	defer processorLock.Unlock()
	processorCount++
	if processorCount <= 1 {
		if v.Debug {
			vips.SetLogging(func(domain string, level vips.LogLevel, msg string) {
				ctx := getCurrentContext()
				logFields := []zap.Field{zap.String("log", msg)}
				if ctx != nil {
					if requestID := imagor.GetRequestID(ctx); requestID != "" {
						logFields = append(logFields, zap.String("request_id", requestID))
					}
				}
				switch level {
				case vips.LogLevelDebug:
					v.Logger.Debug(domain, logFields...)
				case vips.LogLevelMessage, vips.LogLevelInfo:
					v.Logger.Info(domain, logFields...)
				case vips.LogLevelWarning, vips.LogLevelCritical, vips.LogLevelError:
					v.Logger.Warn(domain, logFields...)
				}
			}, vips.LogLevelDebug)
		} else {
			vips.SetLogging(func(domain string, level vips.LogLevel, msg string) {
				ctx := getCurrentContext()
				logFields := []zap.Field{zap.String("log", msg)}
				if ctx != nil {
					if requestID := imagor.GetRequestID(ctx); requestID != "" {
						logFields = append(logFields, zap.String("request_id", requestID))
					}
				}
				v.Logger.Warn(domain, logFields...)
			}, vips.LogLevelError)
		}
		vips.Startup(&vips.Config{
			MaxCacheFiles:    v.MaxCacheFiles,
			MaxCacheMem:      v.MaxCacheMem,
			MaxCacheSize:     v.MaxCacheSize,
			ConcurrencyLevel: v.Concurrency,
		})
	}
	if v.FallbackFunc == nil {
		if vips.HasOperation("magickload_buffer") {
			v.FallbackFunc = bufferFallbackFunc
			v.Logger.Debug("source fallback", zap.String("fallback", "magickload_buffer"))
		} else {
			v.FallbackFunc = v.bmpFallbackFunc
			v.Logger.Debug("source fallback", zap.String("fallback", "bmp"))
		}
	}
	return nil
}

// Shutdown implements imagor.Processor interface
func (v *Processor) Shutdown(_ context.Context) error {
	processorLock.Lock()
	defer processorLock.Unlock()
	if processorCount <= 0 {
		return nil
	}
	processorCount--
	if processorCount == 0 {
		vips.Shutdown()
	}
	return nil
}

func (v *Processor) newImageFromBlob(
	ctx context.Context, blob *imagor.Blob, options *vips.LoadOptions,
) (*vips.Image, error) {
	timer := v.NewMethodTimer("vipsprocessor.newImageFromBlob")
	if timer != nil {
		defer timer.ObserveDuration(ctx)
	}
	if blob == nil || blob.IsEmpty() {
		return nil, imagor.ErrNotFound
	}
	if blob.BlobType() == imagor.BlobTypeMemory {
		buf, width, height, bands, _ := blob.Memory()
		return vips.NewImageFromMemory(buf, width, height, bands)
	}

	// For large PNG files (>1MB), use buffer fallback to avoid streaming issues
	// that can cause row duplication in libvips 8.17.2
	if blob.BlobType() == imagor.BlobTypePNG && blob.Size() > 0 && blob.Size() > v.PNGBufferThreshold {
		buf, err := blob.ReadAll()
		if err != nil {
			return nil, err
		}
		return vips.NewImageFromBuffer(buf, options)
	}

	reader, _, err := blob.NewReader()
	if err != nil {
		return nil, err
	}
	src := vips.NewSource(reader)
	contextDefer(ctx, src.Close)
	img, err := vips.NewImageFromSource(src, options)
	if err != nil && v.FallbackFunc != nil {
		src.Close()
		return v.FallbackFunc(blob, options)
	}
	return img, err
}

func (v *Processor) newThumbnailFromBlob(
	ctx context.Context, blob *imagor.Blob,
	width, height int, crop vips.Interesting, size vips.Size, options *vips.LoadOptions,
) (*vips.Image, error) {
	timer := v.NewMethodTimer("vipsprocessor.newThumbnailFromBlob")
	if timer != nil {
		defer timer.ObserveDuration(ctx)
	}
	if blob == nil || blob.IsEmpty() {
		return nil, imagor.ErrNotFound
	}
	reader, _, err := blob.NewReader()
	if err != nil {
		return nil, err
	}
	src := vips.NewSource(reader)
	contextDefer(ctx, src.Close)
	var optionString string
	if options != nil {
		optionString = options.OptionString()
	}
	return vips.NewThumbnailSource(src, width, &vips.ThumbnailSourceOptions{
		Height:       height,
		Crop:         crop,
		Size:         size,
		OptionString: optionString,
	})
}

// NewThumbnail creates new thumbnail with resize and crop from imagor.Blob
func (v *Processor) NewThumbnail(
	ctx context.Context, blob *imagor.Blob, width, height int, crop vips.Interesting,
	size vips.Size, n, page int, dpi int,
) (*vips.Image, error) {
	timer := v.NewMethodTimer("vipsprocessor.NewThumbnail")
	if timer != nil {
		defer timer.ObserveDuration(ctx)
	}
	var options = &vips.LoadOptions{}
	if dpi > 0 {
		options.Dpi = dpi
	}
	options.Unlimited = v.Unlimited
	var err error
	var img *vips.Image
	if isMultiPage(blob, n, page) {
		applyMultiPageOptions(options, n, page)
		if crop == vips.InterestingNone || size == vips.SizeForce {
			if img, err = v.newImageFromBlob(ctx, blob, options); err != nil {
				return nil, WrapErr(err)
			}
			if n > 1 || page > 1 {
				// reload image to restrict frames loaded
				n, page = recalculateImage(img, n, page)
				return v.NewThumbnail(ctx, blob, width, height, crop, size, -n, -page, dpi)
			}
			if _, err = v.CheckResolution(img, nil); err != nil {
				return nil, err
			}
			if err = img.ThumbnailImage(width, &vips.ThumbnailImageOptions{
				Height: height, Size: size, Crop: crop,
			}); err != nil {
				img.Close()
				return nil, WrapErr(err)
			}
		} else {
			if img, err = v.CheckResolution(v.newImageFromBlob(ctx, blob, options)); err != nil {
				return nil, WrapErr(err)
			}
			if n > 1 || page > 1 {
				// reload image to restrict frames loaded
				n, page = recalculateImage(img, n, page)
				return v.NewThumbnail(ctx, blob, width, height, crop, size, -n, -page, dpi)
			}
			if err = v.animatedThumbnailWithCrop(img, width, height, crop, size); err != nil {
				img.Close()
				return nil, WrapErr(err)
			}
		}
	} else {
		switch blob.BlobType() {
		case imagor.BlobTypeJPEG, imagor.BlobTypeGIF, imagor.BlobTypeWEBP:
			// only allow real thumbnail for jpeg gif webp
			img, err = v.newThumbnailFromBlob(ctx, blob, width, height, crop, size, options)
		default:
			img, err = v.newThumbnailFallback(ctx, blob, width, height, crop, size, options)
		}
	}
	return v.CheckResolution(img, WrapErr(err))
}

func (v *Processor) newThumbnailFallback(
	ctx context.Context, blob *imagor.Blob, width, height int, crop vips.Interesting, size vips.Size, options *vips.LoadOptions,
) (img *vips.Image, err error) {
	timer := v.NewMethodTimer("vipsprocessor.newThumbnailFallback")
	if timer != nil {
		defer timer.ObserveDuration(ctx)
	}
	if img, err = v.CheckResolution(v.newImageFromBlob(ctx, blob, options)); err != nil {
		return
	}
	if err = img.ThumbnailImage(width, &vips.ThumbnailImageOptions{
		Height: height, Size: size, Crop: crop,
	}); err != nil {
		img.Close()
		return
	}
	return img, WrapErr(err)
}

// NewImage creates new Image from imagor.Blob
func (v *Processor) NewImage(ctx context.Context, blob *imagor.Blob, n, page int, dpi int) (*vips.Image, error) {
	timer := v.NewMethodTimer("vipsprocessor.NewImage")
	if timer != nil {
		defer timer.ObserveDuration(ctx)
	}
	var options = &vips.LoadOptions{}
	if dpi > 0 {
		options.Dpi = dpi
	}
	options.Unlimited = v.Unlimited
	if isMultiPage(blob, n, page) {
		applyMultiPageOptions(options, n, page)
		img, err := v.CheckResolution(v.newImageFromBlob(ctx, blob, options))
		if err != nil {
			return nil, WrapErr(err)
		}
		// reload image to restrict frames loaded
		if n > 1 || page > 1 {
			n, page = recalculateImage(img, n, page)
			return v.NewImage(ctx, blob, -n, -page, dpi)
		}
		return img, nil
	}
	img, err := v.CheckResolution(v.newImageFromBlob(ctx, blob, options))
	if err != nil {
		return nil, WrapErr(err)
	}
	return img, nil
}

// Thumbnail handles thumbnail operation
func (v *Processor) Thumbnail(
	ctx context.Context,
	img *vips.Image, width, height int, crop vips.Interesting, size vips.Size,
) error {
	timer := v.NewMethodTimer("vipsprocessor.Thumbnail")
	if timer != nil {
		defer timer.ObserveDuration(ctx)
	}
	if crop == vips.InterestingNone || size == vips.SizeForce || img.Height() == img.PageHeight() {
		return img.ThumbnailImage(width, &vips.ThumbnailImageOptions{
			Height: height, Size: size, Crop: crop,
		})
	}
	return v.animatedThumbnailWithCrop(img, width, height, crop, size)
}

// FocalThumbnail handles thumbnail with custom focal point
func (v *Processor) FocalThumbnail(ctx context.Context, img *vips.Image, w, h int, fx, fy float64) (err error) {
	timer := v.NewMethodTimer("vipsprocessor.FocalThumbnail")
	if timer != nil {
		defer timer.ObserveDuration(ctx)
	}
	var imageWidth, imageHeight float64
	// exif orientation greater 5-8 are 90 or 270 degrees, w and h swapped
	if img.Orientation() > 4 {
		imageWidth = float64(img.PageHeight())
		imageHeight = float64(img.Width())
	} else {
		imageWidth = float64(img.Width())
		imageHeight = float64(img.PageHeight())
	}

	if float64(w)/float64(h) > float64(imageWidth)/float64(imageHeight) {
		if err = img.ThumbnailImage(w, &vips.ThumbnailImageOptions{
			Height: v.MaxHeight, Crop: vips.InterestingNone,
		}); err != nil {
			return
		}
	} else {
		if err = img.ThumbnailImage(v.MaxWidth, &vips.ThumbnailImageOptions{
			Height: h, Crop: vips.InterestingNone,
		}); err != nil {
			return
		}
	}
	var top, left float64
	left = float64(img.Width())*fx - float64(w)/2
	top = float64(img.PageHeight())*fy - float64(h)/2
	left = math.Max(0, math.Min(left, float64(img.Width()-w)))
	top = math.Max(0, math.Min(top, float64(img.PageHeight()-h)))
	return img.ExtractAreaMultiPage(int(left), int(top), w, h)
}

func (v *Processor) animatedThumbnailWithCrop(
	img *vips.Image, w, h int, crop vips.Interesting, size vips.Size,
) (err error) {
	if size == vips.SizeDown && img.Width() < w && img.PageHeight() < h {
		return
	}
	var top, left int
	if float64(w)/float64(h) > float64(img.Width())/float64(img.PageHeight()) {
		if err = img.ThumbnailImage(w, &vips.ThumbnailImageOptions{
			Height: v.MaxHeight, Crop: vips.InterestingNone, Size: size,
		}); err != nil {
			return
		}
	} else {
		if err = img.ThumbnailImage(v.MaxWidth, &vips.ThumbnailImageOptions{
			Height: h, Crop: vips.InterestingNone, Size: size,
		}); err != nil {
			return
		}
	}
	if crop == vips.InterestingHigh {
		left = img.Width() - w
		top = img.PageHeight() - h
	} else if crop == vips.InterestingCentre || crop == vips.InterestingAttention {
		left = (img.Width() - w) / 2
		top = (img.PageHeight() - h) / 2
	}
	return img.ExtractAreaMultiPage(left, top, w, h)
}

// CheckResolution check image resolution for image bomb prevention
func (v *Processor) CheckResolution(img *vips.Image, err error) (*vips.Image, error) {
	if err != nil || img == nil {
		return img, err
	}
	if !v.Unlimited && (img.Width() > v.MaxWidth || img.PageHeight() > v.MaxHeight ||
		(img.Width()*img.Height()) > v.MaxResolution) {
		img.Close()
		return nil, imagor.ErrMaxResolutionExceeded
	}
	return img, nil
}

func isMultiPage(blob *imagor.Blob, n, page int) bool {
	return blob != nil && (blob.SupportsAnimation() || blob.BlobType() == imagor.BlobTypePDF) && ((n != 1 && n != 0) || (page != 1 && page != 0))
}

func applyMultiPageOptions(params *vips.LoadOptions, n, page int) {
	if page < -1 {
		params.Page = -page - 1
	} else if n < -1 {
		params.N = -n
	} else {
		params.N = -1
	}
}

func recalculateImage(img *vips.Image, n, page int) (int, int) {
	// reload image to restrict frames loaded
	numPages := img.Pages()
	img.Close()
	if page > 1 && page > numPages {
		page = numPages
	} else if n > 1 && n > numPages {
		n = numPages
	}
	return n, page
}

// WrapErr wraps error to become imagor.Error
func WrapErr(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(imagor.Error); ok {
		return e
	}
	msg := strings.TrimSpace(err.Error())
	if strings.HasPrefix(msg, "VipsForeignLoad:") &&
		strings.HasSuffix(msg, "is not in a known format") {
		return imagor.ErrUnsupportedFormat
	}
	// send error to sentry
	sentry.CaptureException(err)
	return imagor.NewError(msg, 406)
}
