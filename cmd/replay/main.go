package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/officialasishkumar/streamforge/internal/config"
	"github.com/officialasishkumar/streamforge/internal/types"
	"golang.org/x/time/rate"
)

func main() {
	var tenantFilter string
	var fromISO string
	var toISO string
	var replayRPS int
	flag.StringVar(&tenantFilter, "tenant", "", "optional tenant filter")
	flag.StringVar(&fromISO, "from", "", "optional lower RFC3339 timestamp bound")
	flag.StringVar(&toISO, "to", "", "optional upper RFC3339 timestamp bound")
	flag.IntVar(&replayRPS, "rps", 250, "max replay events per second")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	cfg, err := config.Load("streamforge.yaml")
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}

	from, to := parseTimeBounds(fromISO, toISO)
	limiter := rate.NewLimiter(rate.Limit(replayRPS), replayRPS)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.S3.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		log.Error("load aws config", "error", err)
		os.Exit(1)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3.Endpoint)
			o.UsePathStyle = true
		}
	})

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": strings.Join(cfg.Kafka.Brokers, ",")})
	if err != nil {
		log.Error("create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.S3.Bucket),
		Prefix: aws.String(cfg.S3.ArchivePrefix),
	})

	replayed := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Error("list archive objects", "error", err)
			os.Exit(1)
		}
		for _, obj := range page.Contents {
			if (from != nil && obj.LastModified.Before(*from)) || (to != nil && obj.LastModified.After(*to)) {
				continue
			}
			out, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(cfg.S3.Bucket),
				Key:    obj.Key,
			})
			if err != nil {
				log.Error("fetch archive object", "error", err, "key", aws.ToString(obj.Key))
				continue
			}

			var payload struct {
				TenantID string        `json:"tenant_id"`
				Events   []types.Event `json:"events"`
			}
			if err := json.NewDecoder(out.Body).Decode(&payload); err != nil {
				_ = out.Body.Close()
				log.Error("decode archive object", "error", err, "key", aws.ToString(obj.Key))
				continue
			}
			_ = out.Body.Close()

			if tenantFilter != "" && payload.TenantID != tenantFilter {
				continue
			}
			for _, ev := range payload.Events {
				if err := limiter.Wait(ctx); err != nil {
					log.Error("replay limiter wait", "error", err)
					os.Exit(1)
				}
				b, _ := json.Marshal(ev)
				topic := cfg.Kafka.Topics.Events
				if err := producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Key:            []byte(payload.TenantID),
					Value:          b,
				}, nil); err != nil {
					log.Error("publish replay event", "error", err)
					continue
				}
				replayed++
			}
			producer.Flush(3000)
		}
	}

	log.Info("replay complete", "events_replayed", replayed, "tenant_filter", tenantFilter)
}

func parseTimeBounds(fromISO, toISO string) (*time.Time, *time.Time) {
	var from *time.Time
	var to *time.Time
	if fromISO != "" {
		if t, err := time.Parse(time.RFC3339, fromISO); err == nil {
			from = &t
		}
	}
	if toISO != "" {
		if t, err := time.Parse(time.RFC3339, toISO); err == nil {
			to = &t
		}
	}
	return from, to
}

