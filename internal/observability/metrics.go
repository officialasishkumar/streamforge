package observability

import "github.com/prometheus/client_golang/prometheus"

var (
	EventsReceivedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_events_received_total",
			Help: "Total number of events received by ingest, partitioned by tenant, event type, and result.",
		},
		[]string{"tenant", "event_type", "result"},
	)
	EventsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_events_processed_total",
			Help: "Total events processed by workers, partitioned by tenant and result.",
		},
		[]string{"tenant", "result"},
	)
	KafkaConsumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "streamforge_kafka_consumer_lag",
			Help: "Kafka consumer lag by topic and partition.",
		},
		[]string{"topic", "partition"},
	)
	PostgresPoolInUse = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "streamforge_postgres_pool_in_use", Help: "Postgres pool connections currently in use."},
	)
	PostgresPoolIdle = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "streamforge_postgres_pool_idle", Help: "Postgres pool idle connections."},
	)
	PostgresPoolWaiting = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "streamforge_postgres_pool_waiting", Help: "Postgres pool waiting acquisition count."},
	)
	CircuitBreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "streamforge_circuit_breaker_state",
			Help: "Circuit breaker state by dependency (0=closed, 1=half-open, 2=open).",
		},
		[]string{"dependency"},
	)
	RequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "streamforge_request_duration_seconds",
			Help:    "Request latency for ingest endpoints.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"method", "route", "status"},
	)
)

func init() {
	prometheus.MustRegister(
		EventsReceivedTotal,
		EventsProcessedTotal,
		KafkaConsumerLag,
		PostgresPoolInUse,
		PostgresPoolIdle,
		PostgresPoolWaiting,
		CircuitBreakerState,
		RequestDurationSeconds,
	)
}
