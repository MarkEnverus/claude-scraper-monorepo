# MISO CSAT Supply & Demand Scraper

Collects Capacity Short-Term Adequacy Tracking (CSAT) Supply & Demand data from MISO's Public API.

## Overview

**Data Source:** MISO Public API
**Endpoint:** `https://public-api.misoenergy.org/api/CsatSupplyDemand`
**Update Frequency:** Every 15 minutes
**Data Type:** JSON
**Authentication:** None (public API)

### What is CSAT?

CSAT (Capacity Short-Term Adequacy Tracking) monitors near-term resource adequacy by comparing actual and forecasted demand against committed and available generation capacity. The dataset includes:

- **Actual Demand**: Current load on the grid
- **Committed Capacity**: Generation resources committed to serving load
- **Available Capacity**: Margin between committed capacity and actual demand
- **24-Hour Forecast**: Forecasted demand and capacity for the next 24 hours
- **Adequacy Score**: 0.0-1.0 metric indicating reliability level (higher = better)

This data helps operators and market participants understand near-term reliability margins and grid adequacy conditions.

## Features

- HTTP REST API collection using BaseCollector framework
- No authentication required (public API)
- Snapshot-based collection with 24-hour forecast horizon
- Comprehensive validation:
  - committedCapacity >= actualDemand
  - availableCapacity = committedCapacity - actualDemand
  - adequacyScore value between 0.0 and 1.0
  - All capacity/demand values non-negative
  - All units must be "MW"
- Redis-based hash deduplication
- S3 storage with timestamp partitioning and gzip compression
- Kafka notifications for downstream processing
- Optional region filtering (CENTRAL, SOUTH, NORTH, or MISO_TOTAL)

## Installation

### Prerequisites

- Python 3.9+
- Redis server (for deduplication)
- AWS credentials configured (for S3 storage)

### Dependencies

Install required packages:

```bash
pip install boto3 click redis requests
```

## Usage

### Basic Usage

Collect data for a single day (every 15 minutes):

```bash
python scraper_miso_csat_supply_demand.py \
    --start-datetime 2025-12-05T00:00:00 \
    --end-datetime 2025-12-05T23:59:59
```

### Collect Data for Specific Region

```bash
python scraper_miso_csat_supply_demand.py \
    --start-datetime 2025-12-05T00:00:00 \
    --end-datetime 2025-12-05T23:59:59 \
    --region SOUTH
```

Available regions:
- `CENTRAL` - MISO Central region
- `SOUTH` - MISO South region
- `NORTH` - MISO North region
- Omit for `MISO_TOTAL` (default)

### Using Environment Variables

```bash
export S3_BUCKET=your-bucket
export AWS_PROFILE=your-profile
export REDIS_HOST=localhost
export REDIS_PORT=6379

python scraper_miso_csat_supply_demand.py \
    --start-datetime 2025-12-05T00:00:00 \
    --end-datetime 2025-12-05T23:59:59
```

### Collect Single Hour with Debug Logging

```bash
python scraper_miso_csat_supply_demand.py \
    --start-datetime 2025-12-05T10:00:00 \
    --end-datetime 2025-12-05T11:00:00 \
    --log-level DEBUG
```

### Force Re-download Existing Data

```bash
python scraper_miso_csat_supply_demand.py \
    --start-datetime 2025-12-05T10:00:00 \
    --end-datetime 2025-12-05T11:00:00 \
    --force
```

## Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--start-datetime` | Start timestamp (UTC) in format YYYY-MM-DDTHH:MM:SS | Required |
| `--end-datetime` | End timestamp (UTC) in format YYYY-MM-DDTHH:MM:SS | Required |
| `--region` | Region filter (CENTRAL, SOUTH, NORTH) | None (MISO_TOTAL) |
| `--s3-bucket` | S3 bucket for storage | From `S3_BUCKET` env var |
| `--aws-profile` | AWS profile name | From `AWS_PROFILE` env var |
| `--redis-host` | Redis host | `localhost` |
| `--redis-port` | Redis port | `6379` |
| `--redis-db` | Redis database number | `0` |
| `--environment` | Environment (dev/staging/prod) | `dev` |
| `--force` | Force re-download existing files | `False` |
| `--skip-hash-check` | Skip Redis hash deduplication | `False` |
| `--log-level` | Logging level (DEBUG/INFO/WARNING/ERROR) | `INFO` |

## Data Format

### API Response Structure

```json
{
  "timestamp": "2025-12-05T10:00:00-05:00",
  "region": "MISO_TOTAL",
  "data": {
    "actualDemand": {
      "value": 75000,
      "unit": "MW",
      "timestamp": "2025-12-05T10:00:00-05:00"
    },
    "committedCapacity": {
      "value": 85000,
      "unit": "MW",
      "timestamp": "2025-12-05T10:00:00-05:00"
    },
    "availableCapacity": {
      "value": 10000,
      "unit": "MW",
      "timestamp": "2025-12-05T10:00:00-05:00",
      "calculationType": "committed_minus_demand"
    },
    "demandForecast": {
      "value": 78000,
      "unit": "MW",
      "timestamp": "2025-12-05T10:00:00-05:00",
      "forecastHorizon": "24h"
    },
    "committedCapacityForecast": {
      "value": 87000,
      "unit": "MW",
      "timestamp": "2025-12-05T10:00:00-05:00",
      "forecastHorizon": "24h"
    }
  },
  "adequacyScore": {
    "value": 0.85,
    "interpretation": "Adequate capacity margin",
    "riskLevel": "LOW"
  }
}
```

### Stored File Structure

Files are stored with augmented metadata:

```json
{
  "collection_timestamp": "2025-12-05T10:00:00Z",
  "api_response": {
    // Original API response here
  },
  "metadata": {
    "data_type": "csat_supply_demand",
    "source": "miso",
    "timestamp": "20251205T100000Z",
    "region": "MISO_TOTAL",
    "update_frequency": "15min",
    "forecast_horizon": "24h"
  }
}
```

## S3 Storage Structure

Files are stored with timestamp-based partitioning:

```
s3://{bucket}/sourcing/miso_csat_supply_demand/
  year=2025/
    month=12/
      day=05/
        csat_supply_demand_20251205T100000Z.json.gz
        csat_supply_demand_20251205T101500Z.json.gz
        csat_supply_demand_20251205T103000Z.json.gz
        ...
```

For region-specific data:

```
csat_supply_demand_20251205T100000Z_south.json.gz
csat_supply_demand_20251205T100000Z_central.json.gz
```

## Validation Rules

The scraper validates all collected data:

1. **Structure Validation**
   - All required fields present (timestamp, region, data, adequacyScore)
   - Nested data fields complete (actualDemand, committedCapacity, etc.)

2. **Capacity Constraints**
   - `committedCapacity >= actualDemand` (must be true)
   - `availableCapacity = committedCapacity - actualDemand` (within 1 MW tolerance)

3. **Value Constraints**
   - All capacity/demand values >= 0
   - adequacyScore value between 0.0 and 1.0
   - All units must be "MW"

4. **Forecast Fields**
   - demandForecast and committedCapacityForecast must have forecastHorizon field

## Testing

Run the test suite:

```bash
pytest sourcing/scraping/miso/csat_supply_demand/tests/ -v
```

Run with coverage:

```bash
pytest sourcing/scraping/miso/csat_supply_demand/tests/ -v --cov=sourcing.scraping.miso.csat_supply_demand
```

## Error Handling

The scraper handles various error conditions:

- **404 Not Found**: Treated as "no data available" (not an error)
- **400 Bad Request**: Invalid region parameter
- **429 Rate Limit**: Logs warning about rate limiting
- **Network Errors**: Retries can be configured externally
- **Validation Failures**: Logged and counted in results

## Monitoring

The scraper provides structured logging:

```python
{
  "start_datetime": "2025-12-05T00:00:00",
  "end_datetime": "2025-12-05T23:59:59",
  "region": "MISO_TOTAL",
  "environment": "dev",
  "collected": 96,
  "skipped_duplicate": 0,
  "failed": 0,
  "total_processed": 96
}
```

## Infrastructure

Uses the BaseCollector framework from `sourcing.infrastructure.collection_framework`:

- **Hash Registry**: Redis-based deduplication
- **S3 Storage**: Date-partitioned with gzip compression
- **Kafka Notifications**: Optional downstream notifications

## Troubleshooting

### Redis Connection Failed

```bash
# Check Redis is running
redis-cli ping

# Should return PONG
```

### AWS Credentials Not Found

```bash
# Configure AWS credentials
aws configure --profile your-profile

# Or set environment variables
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
```

### No Data Available (404)

This is normal - CSAT data may not be available for all time periods. The scraper handles 404s gracefully by storing a placeholder record.

## API Documentation

For more information about the MISO Public API:
- **Base URL**: https://public-api.misoenergy.org
- **Documentation**: https://www.misoenergy.org/markets-and-operations/real-time--market-data/

## Version History

- **1.0.0** (2025-12-05): Initial release
  - Snapshot-based collection
  - Comprehensive validation
  - Region filtering support
  - 15-minute interval collection

## License

Internal use only.
