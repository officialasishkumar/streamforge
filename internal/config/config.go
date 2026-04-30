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
	Brokers             []string         `mapstructure:"brokers"`
	Topics              KafkaTopics      `mapstructure:"topics"`
	PartitionerStrategy string           `mapstructure:"partitioner_strategy"`
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

	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("config: validate: %w", err)
	}

	return cfg, nil
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
