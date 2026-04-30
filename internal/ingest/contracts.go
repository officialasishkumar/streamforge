package ingest

import "context"

type ReadinessChecker interface {
	Ready(ctx context.Context) error
}
