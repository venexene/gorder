// Package metrics defines and registers Prometheus counters, gauges, and histograms.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus metrics for the application.
type Metrics struct {
	OrdersProcessed prometheus.Counter
	CacheHits       prometheus.Counter
	CacheMisses     prometheus.Counter
	OrdersInCache   prometheus.Gauge
	RequestDuration *prometheus.HistogramVec
}

// NewMetrics creates and registers all Prometheus metrics.
func NewMetrics() *Metrics {
	ordersProcessed := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "orders_processed_total",
			Help: "Total number of processed orders",
		},
	)
	prometheus.MustRegister(ordersProcessed)

	cacheHits := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "cache_hits_total",
			Help: "Total number of cache hits",
		},
	)
	prometheus.MustRegister(cacheHits)

	cacheMisses := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "cache_misses_total",
			Help: "Total number of cache misses",
		},
	)
	prometheus.MustRegister(cacheMisses)

	ordersInCache := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "orders_in_cache",
			Help: "Gauge of orders in cache",
		},
	)
	prometheus.MustRegister(ordersInCache)

	requestDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gorder_http_request_duration_seconds",
			Help:    "Custom gorder http request duration",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"method", "endpoint", "status"},
	)
	prometheus.MustRegister(requestDuration)

	return &Metrics{
		OrdersProcessed: ordersProcessed,
		CacheHits:       cacheHits,
		CacheMisses:     cacheMisses,
		OrdersInCache:   ordersInCache,
		RequestDuration: requestDuration,
	}
}
