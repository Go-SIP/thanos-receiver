package web

import (
	"context"
	"fmt"
	stdlog "log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Go-SIP/thanos-receiver/receive"
	"github.com/cockroachdb/cmux"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	conntrack "github.com/mwitkow/go-conntrack"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/tsdb"
	"golang.org/x/net/netutil"
)

var (
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "prometheus_http_request_duration_seconds",
			Help:    "Histogram of latencies for HTTP requests.",
			Buckets: []float64{.1, .2, .4, 1, 3, 8, 20, 60, 120},
		},
		[]string{"handler"},
	)
	responseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "prometheus_http_response_size_bytes",
			Help:    "Histogram of response size for HTTP requests.",
			Buckets: prometheus.ExponentialBuckets(100, 10, 8),
		},
		[]string{"handler"},
	)
)

// Appendable returns an Appender.
type Appendable interface {
	Appender() (storage.Appender, error)
}

// Options for the web Handler.
type Options struct {
	Receiver       *receive.Receiver
	Context        context.Context
	ListenAddress  string
	MaxConnections int
	ReadTimeout    time.Duration
	Registry       prometheus.Registerer
	ReadyStorage   *tsdb.ReadyStorage
}

// Handler serves various HTTP endpoints of the Prometheus server
type Handler struct {
	readyStorage *tsdb.ReadyStorage
	context      context.Context
	logger       log.Logger
	receiver     *receive.Receiver
	router       *route.Router
	options      *Options
	quitCh       chan struct{}

	ready uint32 // ready is uint32 rather than boolean to be able to use atomic functions.
}

func instrumentHandler(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
	return promhttp.InstrumentHandlerDuration(
		requestDuration.MustCurryWith(prometheus.Labels{"handler": handlerName}),
		promhttp.InstrumentHandlerResponseSize(
			responseSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
			handler,
		),
	)
}

func New(logger log.Logger, o *Options) *Handler {
	router := route.New().WithInstrumentation(instrumentHandler)
	if logger == nil {
		logger = log.NewNopLogger()
	}

	h := &Handler{
		logger:       logger,
		router:       router,
		readyStorage: o.ReadyStorage,
		context:      o.Context,
		receiver:     o.Receiver,
		options:      o,
		quitCh:       make(chan struct{}),
	}

	readyf := h.testReady
	router.Post("/receive", readyf(h.receive))

	return h
}

// Ready sets Handler to be ready.
func (h *Handler) Ready() {
	atomic.StoreUint32(&h.ready, 1)
}

// Verifies whether the server is ready or not.
func (h *Handler) isReady() bool {
	ready := atomic.LoadUint32(&h.ready)
	return ready > 0
}

// Checks if server is ready, calls f if it is, returns 503 if it is not.
func (h *Handler) testReady(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.isReady() {
			f(w, r)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "Service Unavailable")
		}
	}
}

// Quit returns the receive-only quit channel.
func (h *Handler) Quit() <-chan struct{} {
	return h.quitCh
}

// Checks if server is ready, calls f if it is, returns 503 if it is not.
func (h *Handler) testReadyHandler(f http.Handler) http.HandlerFunc {
	return h.testReady(f.ServeHTTP)
}

// Run serves the HTTP endpoints.
func (h *Handler) Run(ctx context.Context) error {
	level.Info(h.logger).Log("msg", "Start listening for connections", "address", h.options.ListenAddress)

	listener, err := net.Listen("tcp", h.options.ListenAddress)
	if err != nil {
		return err
	}
	listener = netutil.LimitListener(listener, h.options.MaxConnections)

	// Monitor incoming connections with conntrack.
	listener = conntrack.NewListener(listener,
		conntrack.TrackWithName("http"),
		conntrack.TrackWithTracing())

	var (
		m     = cmux.New(listener)
		httpl = m.Match(cmux.HTTP1Fast())
	)

	operationName := nethttp.OperationNameFunc(func(r *http.Request) string {
		return fmt.Sprintf("%s %s", r.Method, r.URL.Path)
	})
	mux := http.NewServeMux()
	mux.Handle("/", h.router)

	errlog := stdlog.New(log.NewStdlibAdapter(level.Error(h.logger)), "", 0)

	httpSrv := &http.Server{
		Handler:     nethttp.Middleware(opentracing.GlobalTracer(), mux, operationName),
		ErrorLog:    errlog,
		ReadTimeout: h.options.ReadTimeout,
	}

	errCh := make(chan error)
	go func() {
		errCh <- httpSrv.Serve(httpl)
	}()
	go func() {
		errCh <- m.Serve()
	}()

	select {
	case e := <-errCh:
		return e
	case <-ctx.Done():
		httpSrv.Shutdown(ctx)
		return nil
	}
}
