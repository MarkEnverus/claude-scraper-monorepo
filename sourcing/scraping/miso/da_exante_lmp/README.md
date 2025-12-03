# MISO Day-Ahead Ex-Ante LMP Scraper

Collects Day-Ahead Ex-Ante Locational Marginal Pricing (LMP) data from MISO's public market reports portal.

## Overview

**Data Source**: MISO Market Reports
**URL Pattern**: `https://docs.misoenergy.org/marketreports/{YYYYMMDD}_da_exante_lmp.csv`
**Update Frequency**: Daily (one file per day)
**Historical Support**: Yes (date-based URLs)
**Authentication**: None required

## What is Day-Ahead Ex-Ante LMP?

Day-Ahead Ex-Ante LMP represents forecasted Locational Marginal Prices for the day-ahead market. These prices are published before the operating day and reflect the expected cost of delivering electricity to specific locations (nodes) in the MISO grid.

## Installation

```bash
cd sourcing/scraping/miso/da_exante_lmp
pip install -r requirements.txt
```

## Configuration

```bash
# Redis Configuration
export REDIS_HOST=localhost
export REDIS_PORT=6379

# AWS Configuration
export S3_BUCKET=scraper-testing
export AWS_PROFILE=localstack  # or your AWS profile
```

## Usage

### Collect Single Day
```bash
python scraper_miso_da_exante_lmp.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-01 \
  --s3-bucket scraper-testing \
  --aws-profile localstack
```

### Collect Date Range (Last 30 Days)
```bash
python scraper_miso_da_exante_lmp.py \
  --start-date 2025-11-03 \
  --end-date 2025-12-02 \
  --s3-bucket scraper-testing \
  --aws-profile localstack \
  --environment dev \
  --log-level INFO
```

### With Kafka Notifications
```bash
export KAFKA_CONNECTION_STRING="user:password@kafka-broker:9092"

python scraper_miso_da_exante_lmp.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-02 \
  --s3-bucket production-data \
  --environment prod
```

## CLI Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--start-date` | Yes | - | Start date (YYYY-MM-DD) |
| `--end-date` | Yes | - | End date (YYYY-MM-DD) |
| `--s3-bucket` | Yes | - | S3 bucket for storage |
| `--aws-profile` | No | default | AWS profile |
| `--environment` | No | dev | Environment (dev/staging/prod) |
| `--redis-host` | No | localhost | Redis host |
| `--redis-port` | No | 6379 | Redis port |
| `--log-level` | No | INFO | Logging level |
| `--kafka-connection-string` | No | - | Kafka connection for notifications |

## Data Format

### CSV Structure
```csv
Node,Type,Value,MarketDay,HourEnding,PNODE
ALTW.ALTW7_,Load,35.21,12/02/2025,1,ALTW.ALTW7
ALTW.ALTW7_,Load,34.89,12/02/2025,2,ALTW.ALTW7
```

### Fields
- **Node**: Node identifier
- **Type**: Node type (Load, Generation)
- **Value**: LMP value in $/MWh
- **MarketDay**: Market day (MM/DD/YYYY)
- **HourEnding**: Hour ending (1-24)
- **PNODE**: Pricing node identifier

## Output

### S3 Storage Path
```
s3://{bucket}/sourcing/miso_da_exante_lmp/
└── 2025/
    └── 12/
        └── 02/
            └── {hash}_da_exante_lmp_20251202.csv.gz
```

### dgroup
`miso_da_exante_lmp`

## Architecture

- **Framework**: BaseCollector (v1.3.0)
- **Deduplication**: Redis-based hash registry
- **Storage**: S3 with gzip compression
- **Notifications**: Optional Kafka streaming
- **Error Handling**: Automatic retries, validation

## Monitoring

Key metrics from collection results:
- `total_candidates`: Number of dates to collect
- `collected`: Successfully collected files
- `skipped_duplicate`: Files already in S3 (deduplicated)
- `failed`: Collection failures

## References

- [MISO Market Reports](https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/)
- [Day-Ahead Market Overview](https://www.misoenergy.org/markets-and-operations/markets/day-ahead-market/)
