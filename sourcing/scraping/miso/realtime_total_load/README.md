# MISO Real-Time Total Load Scraper

## Overview

Collects real-time total load data from MISO's Public API, including cleared capacity, load forecasts, and 5-minute actual load measurements.

**API Endpoint:** `https://public-api.misoenergy.org/api/RealTimeTotalLoad`

**Authentication:** None (Public API)

**Data Update Frequency:** Every 5 minutes

## Data Description

The API provides three types of load data:

1. **ClearedMW**: Hourly cleared megawatt capacity values
   - Format: Array of `{Hour, Value}` objects
   - Example: `{"Hour": "12:00", "Value": 32456}`

2. **MediumTermLoadForecast**: Hourly load forecast for the next 24 hours
   - Format: Array of `{HourEnding, LoadForecast}` objects
   - Example: `{"HourEnding": "14:00", "LoadForecast": 35678}`

3. **FiveMinTotalLoad**: Real-time actual load measurements at 5-minute intervals
   - Format: Array of `{Time, Value}` objects
   - Example: `{"Time": "12:35:00", "Value": 33456}`

Each response includes:
- **RefId**: ISO 8601 timestamp indicating when the dataset was generated
- All three data arrays as described above

## Features

- No authentication required (public API)
- Automatic 5-minute interval collection for continuous monitoring
- Redis-based hash deduplication
- S3 storage with date partitioning (year/month/day)
- Comprehensive JSON validation
- Gzip compression for efficient storage
- Optional Kafka notifications

## Installation

Ensure the infrastructure is set up:

```bash
# Required Python packages
pip install boto3 click redis requests

# Required infrastructure
# - sourcing/infrastructure/collection_framework.py
# - sourcing/infrastructure/hash_registry.py
```

## Usage

### Basic Usage

```bash
# Collect a single snapshot
python scraper_miso_realtime_total_load.py \
    --start-date 2025-12-05T12:00:00 \
    --end-date 2025-12-05T12:00:00
```

### Collect Data for Extended Period

```bash
# Collect data for a full day at 5-minute intervals (288 snapshots)
python scraper_miso_realtime_total_load.py \
    --start-date 2025-12-05 \
    --end-date 2025-12-06
```

### Use Environment Variables

```bash
export S3_BUCKET=your-bucket-name
export REDIS_HOST=localhost
export REDIS_PORT=6379

python scraper_miso_realtime_total_load.py \
    --start-date 2025-12-05T12:00:00 \
    --end-date 2025-12-05T14:00:00
```

### Force Re-download

```bash
python scraper_miso_realtime_total_load.py \
    --start-date 2025-12-05T12:00:00 \
    --end-date 2025-12-05T12:30:00 \
    --force
```

## Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--start-date` | Start date/time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS) | Required |
| `--end-date` | End date/time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS) | Required |
| `--s3-bucket` | S3 bucket for storage (or `S3_BUCKET` env var) | None |
| `--aws-profile` | AWS profile name (or `AWS_PROFILE` env var) | None |
| `--redis-host` | Redis host for deduplication | localhost |
| `--redis-port` | Redis port | 6379 |
| `--redis-db` | Redis database number | 0 |
| `--environment` | Environment (dev/staging/prod) | dev |
| `--force` | Force re-download of existing files | False |
| `--skip-hash-check` | Skip hash-based deduplication | False |
| `--log-level` | Logging level (DEBUG/INFO/WARNING/ERROR) | INFO |

## Data Storage

### S3 Structure

```
s3://{bucket}/sourcing/miso_realtime_total_load/{year}/{month}/{day}/realtime_total_load_{timestamp}.json.gz
```

Example:
```
s3://my-bucket/sourcing/miso_realtime_total_load/2025/12/05/realtime_total_load_20251205_120000.json.gz
```

### File Format

Each file contains:

```json
{
  "data": {
    "RefId": "2025-12-05T12:35:00Z",
    "ClearedMW": [
      {"Hour": "12:00", "Value": 32456},
      {"Hour": "13:00", "Value": 34123}
    ],
    "MediumTermLoadForecast": [
      {"HourEnding": "14:00", "LoadForecast": 35678},
      {"HourEnding": "15:00", "LoadForecast": 36245}
    ],
    "FiveMinTotalLoad": [
      {"Time": "12:35:00", "Value": 33456},
      {"Time": "12:40:00", "Value": 33789}
    ]
  },
  "collection_metadata": {
    "collected_at": "2025-12-05T12:35:12.345678",
    "source_url": "https://public-api.misoenergy.org/api/RealTimeTotalLoad",
    "data_type": "realtime_total_load",
    "source": "miso",
    "date": "2025-12-05",
    "timestamp": "20251205_120000"
  }
}
```

## Validation

The scraper validates:

1. **Structure Validation**
   - Presence of `RefId` field
   - Presence of all three required arrays: `ClearedMW`, `MediumTermLoadForecast`, `FiveMinTotalLoad`
   - Arrays are actually lists, not other types

2. **ClearedMW Records**
   - Required fields: `Hour`, `Value`
   - `Value` must be numeric

3. **MediumTermLoadForecast Records**
   - Required fields: `HourEnding`, `LoadForecast`
   - `LoadForecast` must be numeric

4. **FiveMinTotalLoad Records**
   - Required fields: `Time`, `Value`
   - `Value` must be numeric

5. **Empty Arrays**
   - Empty arrays are considered valid (API may return no data)

## Error Handling

- **404 Not Found**: Data may not be available - logged as warning, raises ScrapingError
- **503 Service Unavailable**: API temporarily down - raises ScrapingError
- **Timeout**: Network timeout - raises ScrapingError
- **Invalid JSON**: Malformed response - raises ScrapingError
- **Validation Failure**: Invalid data structure - logged and skipped

## Testing

### Run Tests

```bash
# Run all tests
pytest sourcing/scraping/miso/realtime_total_load/tests/ -v

# Run with coverage
pytest sourcing/scraping/miso/realtime_total_load/tests/ --cov=sourcing.scraping.miso.realtime_total_load --cov-report=html

# Run specific test
pytest sourcing/scraping/miso/realtime_total_load/tests/test_scraper_miso_realtime_total_load.py::TestMisoRealtimeTotalLoadCollector::test_collect_content_success -v
```

### Test Coverage

The test suite includes:
- Collector initialization
- Candidate generation (single and multiple snapshots)
- Successful content collection
- HTTP error handling (404, 503)
- Invalid JSON handling
- Timeout handling
- Comprehensive validation tests for all data structures
- Edge cases (empty arrays, missing fields, non-numeric values)

## Deduplication

Uses Redis-based content hash deduplication:
- Each file's content is hashed (MD5)
- Hash is stored in Redis with key: `{environment}:{dgroup}:{identifier}`
- Files with identical content hashes are skipped
- Use `--force` to bypass deduplication and re-download
- Use `--skip-hash-check` to disable hash checking entirely

## Data Volume

For continuous collection:
- **5-minute intervals**: 288 snapshots per day
- **Typical file size**: 2-5 KB compressed
- **Daily storage**: ~1 MB compressed

For reference, collecting for 1 year:
- **Files**: ~105,000 snapshots
- **Storage**: ~365 MB compressed

## Production Considerations

1. **Scheduling**: Use cron or similar to run every 5 minutes
   ```bash
   */5 * * * * python /path/to/scraper_miso_realtime_total_load.py --start-date $(date -u +\%Y-\%m-\%dT\%H:\%M:00) --end-date $(date -u +\%Y-\%m-\%dT\%H:\%M:00)
   ```

2. **Monitoring**: Monitor for:
   - API availability (503 errors)
   - Data freshness (RefId timestamps)
   - Collection failures
   - Storage growth

3. **Retention**: Consider data retention policy
   - Real-time data: Keep 7-30 days
   - Aggregated/processed: Keep longer term

4. **Redis**: Ensure Redis persistence for deduplication
   ```bash
   # Redis should have persistence enabled
   save 900 1
   save 300 10
   ```

## Troubleshooting

### Redis Connection Failed

```bash
# Check Redis is running
redis-cli ping

# Start Redis if not running
redis-server
```

### AWS S3 Access Denied

```bash
# Configure AWS credentials
aws configure

# Or use specific profile
python scraper_miso_realtime_total_load.py --aws-profile my-profile ...
```

### API Timeout

```bash
# Check API availability
curl https://public-api.misoenergy.org/api/RealTimeTotalLoad

# If slow, increase timeout (modify TIMEOUT_SECONDS in scraper)
```

## Version History

- **1.0.0** (2025-12-05): Initial release
  - Public API collection
  - 5-minute interval support
  - Complete validation suite
  - Redis deduplication
  - S3 storage with date partitioning

## References

- **API Documentation**: See `/tmp/miso-pricing-specs/spec-public-realtimetotalload.md`
- **MISO Public API**: https://public-api.misoenergy.org/
- **Collection Framework**: `sourcing/infrastructure/collection_framework.py`

## Support

For issues or questions:
1. Check logs with `--log-level DEBUG`
2. Verify Redis connectivity
3. Verify S3 bucket access
4. Test API endpoint availability
5. Review test suite for expected behavior
