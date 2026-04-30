#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${STREAMFORGE_POSTGRES_DSN:-}" ]]; then
  echo "STREAMFORGE_POSTGRES_DSN is required"
  exit 1
fi

year_month="$(date -u -d '+1 month' '+%Y_%m' 2>/dev/null || gdate -u -d '+1 month' '+%Y_%m')"
start_date="$(date -u -d "$(date -u -d '+1 month' '+%Y-%m-01')" '+%Y-%m-%d' 2>/dev/null || gdate -u -d "$(gdate -u -d '+1 month' '+%Y-%m-01')" '+%Y-%m-%d')"
end_date="$(date -u -d "$(date -u -d '+2 month' '+%Y-%m-01')" '+%Y-%m-%d' 2>/dev/null || gdate -u -d "$(gdate -u -d '+2 month' '+%Y-%m-01')" '+%Y-%m-%d')"
partition_name="events_partitioned_${year_month}"

sql="
CREATE TABLE IF NOT EXISTS ${partition_name}
PARTITION OF events_partitioned
FOR VALUES FROM ('${start_date}') TO ('${end_date}');
"

psql "${STREAMFORGE_POSTGRES_DSN}" -v ON_ERROR_STOP=1 -c "${sql}"
echo "created_or_verified_partition=${partition_name}"
