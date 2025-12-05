# MISO Snapshot Scraper

Real-time grid status dashboard collector for MISO's Snapshot API.

## Overview

This scraper collects real-time grid status metrics from MISO's public Snapshot API, providing instantaneous insights into grid performance including demand, forecasts, energy costs, and interchange schedules.

**API Endpoint**: `https://public-api.misoenergy.org/api/Snapshot`
**Authentication**: None (public API)
**Update Frequency**: Real-time (5-minute updates recommended)
**Historical Support**: No (only current snapshot available)

## Collected Metrics

The scraper retrieves 4 key metrics per snapshot:

1. **Peak Demand Forecast** (`peak_demand_forecast`)
   - Predicted maximum electrical load for current/upcoming period
   - Unit: Megawatts (MW)

2. **Current Demand** (`current_demand`)
   - Real-time electrical load across MISO grid
   - Unit: Megawatts (MW)

3. **Marginal Energy Cost** (`marginal_energy_cost`)
   - Cost of generating one additional megawatt at current moment
   - Unit: USD per Megawatt-hour ($/MWh)

4. **Scheduled Interchange** (`scheduled_interchange`)
   - Planned import/export between MISO and neighboring grids
   - Unit: Megawatts (MW)

## Data Format

```json
[
  {
    "t": "Forecasted Peak Demand",
    "v": "92540",
    "d": "2025-12-05T09:35:00-05:00",
    "id": "peak_demand_forecast"
  },
  {
    "t": "Current Demand",
    "v": "91809",
    "d": "2025-12-05T09:35:00-05:00",
    "id": "current_demand"
  },
  {
    "t": "Marginal Energy Cost",
    "v": "39.36",
    "d": "2025-12-05T09:35:00-05:00",
    "id": "marginal_energy_cost"
  },
  {
    "t": "Scheduled Interchange",
    "v": "763",
    "d": "2025-12-05T09:35:00-05:00",
    "id": "scheduled_interchange"
  }
]
```

## Setup

### Prerequisites

- Python 3.10+
- Redis server (for deduplication)
- AWS credentials configured (for S3 storage)
- Required Python packages (see project requirements)

### Environment Variables

```bash
# Required
export S3_BUCKET=your-s3-bucket-name

# Optional
export AWS_PROFILE=your-aws-profile      # For LocalStack or specific AWS profile
export REDIS_HOST=localhost              # Default: localhost
export REDIS_PORT=6379                   # Default: 6379
export REDIS_DB=0                        # Default: 0
export KAFKA_CONNECTION_STRING=...       # Optional: for Kafka notifications
```

## Usage

### Basic Usage

```bash
python scraper_miso_snapshot.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack
```

### With Kafka Notifications

```bash
python scraper_miso_snapshot.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack \
  --kafka-connection-string "user:pass@kafka-broker:9092"
```

### Production Environment

```bash
python scraper_miso_snapshot.py \
  --s3-bucket production-bucket \
  --environment prod \
  --log-level INFO
```

## Command-Line Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `--s3-bucket` | `S3_BUCKET` | (required) | S3 bucket for data storage |
| `--aws-profile` | `AWS_PROFILE` | None | AWS profile name |
| `--environment` | - | `dev` | Environment: dev, staging, prod |
| `--redis-host` | `REDIS_HOST` | `localhost` | Redis server hostname |
| `--redis-port` | `REDIS_PORT` | `6379` | Redis server port |
| `--redis-db` | `REDIS_DB` | `0` | Redis database number |
| `--log-level` | - | `INFO` | Logging level: DEBUG, INFO, WARNING, ERROR |
| `--kafka-connection-string` | `KAFKA_CONNECTION_STRING` | None | Kafka connection for notifications |

## Testing

### Run All Tests

```bash
pytest sourcing/scraping/miso/snapshot/tests/ -v
```

### Run Specific Test Class

```bash
pytest sourcing/scraping/miso/snapshot/tests/test_scraper_miso_snapshot.py::TestContentValidation -v
```

### Run with Coverage

```bash
pytest sourcing/scraping/miso/snapshot/tests/ --cov=sourcing.scraping.miso.snapshot --cov-report=html
```

## Storage

### S3 Structure

Data is stored in S3 with the following structure:

```
s3://{bucket}/sourcing/miso_snapshot/{date}/snapshot_{timestamp}.json.gz
```

Example:
```
s3://scraper-testing/sourcing/miso_snapshot/2025-12-05/snapshot_20251205_0935.json.gz
```

### Data Retention

- Raw data stored compressed (gzip)
- Hash-based deduplication via Redis
- Date-partitioned for efficient querying

## Monitoring & Alerting

### Key Metrics to Monitor

1. **Collection Success Rate**
   - Target: >99% success
   - Alert if failures exceed 1%

2. **Data Freshness**
   - Expected update: Every 5-15 minutes
   - Alert if no new data for 30 minutes

3. **Duplicate Rate**
   - Normal: <5% duplicates
   - Alert if duplicates exceed 20%

4. **Response Time**
   - Typical: <5 seconds
   - Alert if exceeds 30 seconds

### Recommended Scheduling

Run every 5-15 minutes using cron or scheduler:

```bash
*/5 * * * * cd /path/to/repo && python sourcing/scraping/miso/snapshot/scraper_miso_snapshot.py --s3-bucket production-bucket --environment prod
```

## Use Cases

1. **Grid Monitoring**
   - Real-time load tracking
   - Demand forecasting validation
   - Grid health assessment

2. **Energy Trading**
   - Price signal analysis
   - Market opportunity identification
   - Cost optimization

3. **Infrastructure Management**
   - Capacity planning
   - Emergency preparedness
   - Demand response triggers

4. **Analytics & Reporting**
   - Historical trend analysis
   - Performance benchmarking
   - Grid stability metrics

## Error Handling

The scraper includes comprehensive error handling:

- **HTTP Errors**: Automatic retry with exponential backoff
- **Timeout Errors**: 30-second timeout with graceful failure
- **Validation Errors**: Content validation before storage
- **Duplicate Detection**: Redis-based hash checking
- **S3 Failures**: Detailed error logging and alerting

## Architecture

Built on the BaseCollector framework with:

- **Candidate Generation**: Single snapshot per execution
- **Content Collection**: HTTP GET request to public API
- **Content Validation**: JSON structure and metric validation
- **Storage**: Compressed S3 storage with date partitioning
- **Deduplication**: Redis hash registry
- **Notifications**: Optional Kafka notifications

## Troubleshooting

### Issue: Connection Timeout

```bash
# Increase timeout in code or check network connectivity
curl -v https://public-api.misoenergy.org/api/Snapshot
```

### Issue: Redis Connection Failed

```bash
# Verify Redis is running
redis-cli ping

# Check Redis connectivity
telnet localhost 6379
```

### Issue: S3 Access Denied

```bash
# Verify AWS credentials
aws s3 ls s3://your-bucket-name/

# Check AWS profile
aws configure list --profile your-profile
```

### Issue: Validation Failures

```bash
# Test API directly
curl https://public-api.misoenergy.org/api/Snapshot | jq .

# Enable debug logging
python scraper_miso_snapshot.py --log-level DEBUG ...
```

## Related Documentation

- [MISO API Specification](/tmp/miso-pricing-specs/spec-public-snapshot.md)
- [Collection Framework Documentation](../../infrastructure/collection_framework.py)
- [Hash Registry Documentation](../../infrastructure/hash_registry.py)

## Support

For issues or questions:
1. Check logs with `--log-level DEBUG`
2. Review test fixtures for expected data format
3. Verify API endpoint is accessible
4. Check Redis and S3 connectivity

## Version History

- **1.0.0** (2025-12-05): Initial release
  - Real-time snapshot collection
  - 4 key metrics support
  - Redis deduplication
  - S3 storage with compression
  - Comprehensive test coverage
