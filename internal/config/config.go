package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Ingest        IngestConfig        `mapstructure:"ingest"`
	Workers       WorkersConfig       `mapstructure:"workers"`
	Kafka         KafkaConfig         `mapstructure:"kafka"`
	Postgres      PostgresConfig      `mapstructure:"postgres"`
	S3            S3Config            `mapstructure:"s3"`
	SQS           SQSConfig           `mapstructure:"sqs"`
	Redis         RedisConfig         `mapstructure:"redis"`
	Observability ObservabilityConfig `mapstructure:"observability"`
	RateLimits    RateLimitsConfig    `mapstructure:"rate_limits"`
}

type IngestConfig struct {
	Port           int           `mapstructure:"port"`
	MaxBatchSize   int           `mapstructure:"max_batch_size"`
	RequestTimeout time.Duration `mapstructure:"request_timeout"`
}

type WorkersConfig struct {
	PoolSize     int           `mapstructure:"pool_size"`
	BatchSize    int           `mapstructure:"batch_size"`
	FetchTimeout time.Duration `mapstructure:"fetch_timeout"`
}

type KafkaConfig struct {
	Brokers             []string    `mapstructure:"brokers"`
	Topics              KafkaTopics `mapstructure:"topics"`
	PartitionerStrategy string      `mapstructure:"partitioner_strategy"`
}

type KafkaTopics struct {
	Events string `mapstructure:"events"`
	DLQ    string `mapstructure:"dlq"`
	Outbox string `mapstructure:"outbox"`
}

type PostgresConfig struct {
	DSN              string        `mapstructure:"dsn"`
	PoolMin          int32         `mapstructure:"pool_min"`
	PoolMax          int32         `mapstructure:"pool_max"`
	StatementTimeout time.Duration `mapstructure:"statement_timeout"`
}

type S3Config struct {
	Bucket        string `mapstructure:"bucket"`
	Endpoint      string `mapstructure:"endpoint"`
	Region        string `mapstructure:"region"`
	ArchivePrefix string `mapstructure:"archive_prefix"`
}

type SQSConfig struct {
	QueueURL string `mapstructure:"queue_url"`
	Endpoint string `mapstructure:"endpoint"`
	Region   string `mapstructure:"region"`
}

type RedisConfig struct {
	Addr               string `mapstructure:"addr"`
	DB                 int    `mapstructure:"db"`
	RateLimitKeyPrefix string `mapstructure:"ratelimit_key_prefix"`
}

type ObservabilityConfig struct {
	MetricsAddr string  `mapstructure:"metrics_addr"`
	LogLevel    string  `mapstructure:"log_level"`
	SampleRate  float64 `mapstructure:"sample_rate"`
}

type RateLimitsConfig struct {
	DefaultPerTenantRPS int `mapstructure:"default_per_tenant_rps"`
	DefaultBurst        int `mapstructure:"default_burst"`
}

func Load(path string) (Config, error) {
	var cfg Config

	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")
	v.SetEnvPrefix("STREAMFORGE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return cfg, fmt.Errorf("config: read config file: %w", err)
	}

	if err := v.Unmarshal(&cfg); err != nil {
		return cfg, fmt.Errorf("config: unmarshal: %w", err)
	}
	if err := applyResolvedValues(v, &cfg); err != nil {
		return cfg, fmt.Errorf("config: apply resolved values: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("config: validate: %w", err)
	}

	return cfg, nil
}

func applyResolvedValues(v *viper.Viper, cfg *Config) error {
	poolMin, err := int32Setting(v, "postgres.pool_min")
	if err != nil {
		return err
	}
	poolMax, err := int32Setting(v, "postgres.pool_max")
	if err != nil {
		return err
	}

	cfg.Ingest.Port = v.GetInt("ingest.port")
	cfg.Ingest.MaxBatchSize = v.GetInt("ingest.max_batch_size")
	cfg.Ingest.RequestTimeout = v.GetDuration("ingest.request_timeout")
	cfg.Workers.PoolSize = v.GetInt("workers.pool_size")
	cfg.Workers.BatchSize = v.GetInt("workers.batch_size")
	cfg.Workers.FetchTimeout = v.GetDuration("workers.fetch_timeout")
	cfg.Kafka.Brokers = stringSlice(v, "kafka.brokers")
	cfg.Kafka.Topics.Events = v.GetString("kafka.topics.events")
	cfg.Kafka.Topics.DLQ = v.GetString("kafka.topics.dlq")
	cfg.Kafka.Topics.Outbox = v.GetString("kafka.topics.outbox")
	cfg.Kafka.PartitionerStrategy = v.GetString("kafka.partitioner_strategy")
	cfg.Postgres.DSN = v.GetString("postgres.dsn")
	cfg.Postgres.PoolMin = poolMin
	cfg.Postgres.PoolMax = poolMax
	cfg.Postgres.StatementTimeout = v.GetDuration("postgres.statement_timeout")
	cfg.S3.Bucket = v.GetString("s3.bucket")
	cfg.S3.Endpoint = v.GetString("s3.endpoint")
	cfg.S3.Region = v.GetString("s3.region")
	cfg.S3.ArchivePrefix = v.GetString("s3.archive_prefix")
	cfg.SQS.QueueURL = v.GetString("sqs.queue_url")
	cfg.SQS.Endpoint = v.GetString("sqs.endpoint")
	cfg.SQS.Region = v.GetString("sqs.region")
	cfg.Redis.Addr = v.GetString("redis.addr")
	cfg.Redis.DB = v.GetInt("redis.db")
	cfg.Redis.RateLimitKeyPrefix = v.GetString("redis.ratelimit_key_prefix")
	cfg.Observability.MetricsAddr = v.GetString("observability.metrics_addr")
	cfg.Observability.LogLevel = v.GetString("observability.log_level")
	cfg.Observability.SampleRate = v.GetFloat64("observability.sample_rate")
	cfg.RateLimits.DefaultPerTenantRPS = v.GetInt("rate_limits.default_per_tenant_rps")
	cfg.RateLimits.DefaultBurst = v.GetInt("rate_limits.default_burst")
	return nil
}

func stringSlice(v *viper.Viper, key string) []string {
	values := v.GetStringSlice(key)
	if len(values) == 0 {
		values = []string{v.GetString(key)}
	}

	var out []string
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

func int32Setting(v *viper.Viper, key string) (int32, error) {
	const (
		minInt32 = -1 << 31
		maxInt32 = 1<<31 - 1
	)

	value := v.GetInt(key)
	if value < minInt32 || value > maxInt32 {
		return 0, fmt.Errorf("%s is outside int32 range", key)
	}
	return int32(value), nil
}

func (c Config) Validate() error {
	if c.Ingest.Port <= 0 {
		return fmt.Errorf("ingest.port must be positive")
	}
	if c.Ingest.MaxBatchSize <= 0 || c.Ingest.MaxBatchSize > 1000 {
		return fmt.Errorf("ingest.max_batch_size must be between 1 and 1000")
	}
	if c.Ingest.RequestTimeout <= 0 {
		return fmt.Errorf("ingest.request_timeout must be positive")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must not be empty")
	}
	if c.Kafka.Topics.Events == "" || c.Kafka.Topics.DLQ == "" || c.Kafka.Topics.Outbox == "" {
		return fmt.Errorf("kafka.topics events/dlq/outbox are required")
	}
	if c.Postgres.DSN == "" {
		return fmt.Errorf("postgres.dsn is required")
	}
	if c.Postgres.PoolMax <= 0 || c.Postgres.PoolMin < 0 || c.Postgres.PoolMin > c.Postgres.PoolMax {
		return fmt.Errorf("postgres pool bounds are invalid")
	}
	if c.S3.Bucket == "" || c.S3.Region == "" {
		return fmt.Errorf("s3 bucket and region are required")
	}
	if c.SQS.QueueURL == "" || c.SQS.Region == "" {
		return fmt.Errorf("sqs queue_url and region are required")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("redis.addr is required")
	}
	if c.RateLimits.DefaultPerTenantRPS <= 0 || c.RateLimits.DefaultBurst <= 0 {
		return fmt.Errorf("rate limit defaults must be positive")
	}
	if c.Observability.MetricsAddr == "" {
		return fmt.Errorf("observability.metrics_addr is required")
	}
	if c.Observability.LogLevel == "" {
		return fmt.Errorf("observability.log_level is required")
	}
	return nil
}
