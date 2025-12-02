# MISO Net Scheduled Interchange (NSI) Scraper - Five Minute Data

Collects Net Scheduled Interchange (NSI) data from MISO's public API at 5-minute intervals. NSI represents the net amount of power scheduled to flow into or out of MISO's control area.

## Overview

**Data Source**: MISO (Midwest ISO)
**API Endpoint**: `https://public-api.misoenergy.org/api/Interchange/GetNsi/FiveMinute`
**Update Frequency**: Every 5 minutes (real-time)
**Data Format**: JSON
**Authentication**: None required (public API)
**Historical Data**: No - API returns only current/latest data

## What is Net Scheduled Interchange (NSI)?

Net Scheduled Interchange (NSI) is the net amount of electric power scheduled to flow into or out of MISO's control area:

- **Positive NSI**: Net import (more power flowing INTO MISO)
- **Negative NSI**: Net export (more power flowing OUT OF MISO)
- **Units**: Megawatts (MW)
- **Update Frequency**: Every 5 minutes in real-time operations

## Architecture

This scraper uses the **BaseCollector framework** (v1.3.0) which provides:

- ✅ Redis-based hash deduplication
- ✅ S3 storage with gzip compression and date partitioning
- ✅ Optional Kafka notifications for downstream processing
- ✅ Comprehensive error handling and logging
- ✅ Automatic retries and validation

## Installation

### Prerequisites

- Python 3.8+
- Redis (for hash registry)
- AWS S3 access (or LocalStack for testing)
- (Optional) Kafka for streaming

### Install Dependencies

```bash
cd sourcing/scraping/miso/nsi_five_minute
pip install -r requirements.txt
```

## Configuration

### Required Environment Variables

```bash
# S3 Configuration
export S3_BUCKET=your-raw-data-bucket
export AWS_PROFILE=your-aws-profile  # Optional

# Redis Configuration
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB=0

# Optional: Kafka Configuration
export KAFKA_CONNECTION_STRING=user:password@kafka-broker:9092
```

## Usage

### Basic Collection

Collect current NSI data and store in S3:

```bash
python scraper_miso_nsi_five_minute.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack \
  --environment dev
```

### With Kafka Notifications

Enable Kafka for downstream processing:

```bash
python scraper_miso_nsi_five_minute.py \
  --s3-bucket production-data \
  --environment prod \
  --kafka-connection-string "${KAFKA_CONNECTION_STRING}"
```

### Debug Mode

Enable detailed logging:

```bash
python scraper_miso_nsi_five_minute.py \
  --s3-bucket scraper-testing \
  --log-level DEBUG
```

## CLI Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--s3-bucket` | String | *Required* | S3 bucket for storing data |
| `--aws-profile` | String | None | AWS profile (for LocalStack or AWS) |
| `--environment` | Choice | dev | Environment: dev, staging, prod |
| `--redis-host` | String | localhost | Redis host |
| `--redis-port` | Integer | 6379 | Redis port |
| `--redis-db` | Integer | 0 | Redis database number |
| `--log-level` | Choice | INFO | DEBUG, INFO, WARNING, ERROR |
| `--kafka-connection-string` | String | None | Kafka connection for notifications |

## Data Flow

```
1. Generate Candidate
   └─> Single candidate for current NSI data

2. Collect Content
   └─> HTTP GET to MISO API
   └─> Timeout: 30 seconds
   └─> User-Agent: MISO-NSI-Collector/1.0

3. Validate Content
   └─> Verify valid JSON structure
   └─> Check for NSI value fields
   └─> Verify timestamp fields present

4. Hash Check (Redis)
   └─> Calculate SHA256 content hash
   └─> Check if already collected
   └─> Skip if duplicate

5. Store Data
   ├─> S3 File Storage (gzipped)
   │   └─> Path: s3://{bucket}/sourcing/miso_nsi_five_minute/{YYYY}/{MM}/{DD}/{hash}.json.gz
   │
   └─> Kafka Notification (if enabled)
       └─> Topic: miso-nsi-five-minute
       └─> Message: S3 path, hash, metadata
```

## Output Structure

### S3 Storage Path

```
s3://{bucket}/sourcing/miso_nsi_five_minute/
└── 2025/
    └── 12/
        └── 02/
            ├── abc123def456_nsi_five_minute_20251202_1430.json.gz
            ├── abc123def456_nsi_five_minute_20251202_1435.json.gz
            └── ...
```

### File Format

Files are stored as gzipped JSON with the following naming convention:

```
{hash}_{data_type}_{timestamp}.json.gz
```

### Expected API Response

```json
{
  "EffectiveTime": "2025-01-21T14:35:00",
  "Value": -542.5,
  "Unit": "MW",
  "Description": "Net Scheduled Interchange - Five Minute",
  "MarketType": "RealTime",
  "RefreshTime": "2025-01-21T14:36:15"
}
```

**Field Descriptions:**
- `EffectiveTime`: Timestamp for which the NSI value is effective
- `Value`: Net Scheduled Interchange in MW (negative = export, positive = import)
- `Unit`: Measurement unit (typically "MW")
- `Description`: Human-readable description
- `MarketType`: Market context (typically "RealTime")
- `RefreshTime`: When the data was last refreshed

## Testing

### Run All Tests

```bash
cd sourcing/scraping/miso/nsi_five_minute
pytest tests/ -v
```

### Run with Coverage

```bash
pytest tests/ --cov=. --cov-report=html
```

### Test Coverage

- ✅ Collector initialization
- ✅ Candidate generation
- ✅ HTTP content collection (success and failures)
- ✅ Error handling (timeout, HTTP errors, network errors)
- ✅ Content validation (valid/invalid JSON, missing fields, edge cases)
- ✅ Redis deduplication
- ✅ S3 storage

## Scheduling

Recommended collection frequencies based on use case:

### Real-time Operations (Cron)

```bash
# Every 5 minutes (matches API update frequency)
*/5 * * * * cd /path/to/scraper && python scraper_miso_nsi_five_minute.py --s3-bucket production-data --kafka-connection-string "$KAFKA_CONNECTION_STRING"
```

### Standard Collection

```bash
# Every 10 minutes (to reduce API load)
*/10 * * * * cd /path/to/scraper && python scraper_miso_nsi_five_minute.py --s3-bucket production-data
```

### Development/Testing

```bash
# Every 30 minutes
*/30 * * * * cd /path/to/scraper && python scraper_miso_nsi_five_minute.py --s3-bucket scraper-testing --aws-profile localstack
```

## Monitoring

### Key Metrics

1. **Collection Success Rate**: Percentage of successful API calls
2. **Response Time**: Average time to collect data from API (<1s typical)
3. **Duplicate Rate**: Percentage of duplicate content (high is normal)
4. **Error Rate**: Frequency of timeouts, HTTP errors, or validation failures
5. **Data Freshness**: Age of collected data (should be < 5 minutes)

### Log Output

The scraper produces structured logs:

```
2025-12-02 14:30:00 - sourcing_app - INFO - Starting MISO NSI five-minute data collection
2025-12-02 14:30:00 - sourcing_app - INFO - S3 Bucket: production-data
2025-12-02 14:30:00 - sourcing_app - INFO - Connected to Redis at localhost:6379
2025-12-02 14:30:00 - sourcing_app - INFO - Generated candidate: nsi_five_minute_20251202_1430.json
2025-12-02 14:30:01 - sourcing_app - INFO - Successfully fetched 245 bytes (response time: 0.48s)
2025-12-02 14:30:01 - sourcing_app - INFO - Content validation passed (dict with 6 fields)
2025-12-02 14:30:01 - sourcing_app - INFO - Collection completed: {...}
```

## Error Handling

### Common Errors and Solutions

| Error | Cause | Resolution |
|-------|-------|------------|
| `Timeout fetching NSI data` | API slow/unavailable | Retry, check network |
| `HTTP error (status 503)` | API maintenance | Wait and retry |
| `Invalid JSON` | API format changed | Update validation logic |
| `Redis connection failed` | Redis unavailable | Check Redis config |
| `S3 upload error` | AWS credentials issue | Verify AWS setup |
| `Skipped (duplicate)` | Content already collected | Normal - hash deduplication working |

### Debug Tips

```bash
# Test API connectivity
curl -v https://public-api.misoenergy.org/api/Interchange/GetNsi/FiveMinute

# Test Redis
redis-cli -h $REDIS_HOST -p $REDIS_PORT ping

# Test AWS S3
aws s3 ls s3://$S3_BUCKET/ --profile $AWS_PROFILE

# Run scraper with debug logging
python scraper_miso_nsi_five_minute.py \
  --s3-bucket scraper-testing \
  --log-level DEBUG
```

## Development

### Project Structure

```
sourcing/scraping/miso/nsi_five_minute/
├── scraper_miso_nsi_five_minute.py    # Main scraper (308 lines)
├── requirements.txt                    # Dependencies
├── README.md                          # This file
├── .scraper-dev.md                    # Configuration metadata
└── tests/
    ├── __init__.py
    ├── test_scraper_miso_nsi_five_minute.py  # Tests (350+ lines)
    └── fixtures/
        └── sample_nsi_response.json   # Sample API response
```

### Code Quality

```bash
# Type checking
mypy scraper_miso_nsi_five_minute.py

# Linting
ruff check scraper_miso_nsi_five_minute.py

# Auto-fix
ruff check --fix scraper_miso_nsi_five_minute.py
```

## Related Scrapers

- **MISO Wind Forecast**: `sourcing/scraping/miso/wind_forecast/`

## Infrastructure Version

- **Version**: 1.3.0
- **Last Updated**: 2025-12-02
- **Framework**: BaseCollector (sourcing.infrastructure.collection_framework)

## References

- [MISO Public API](https://www.misoenergy.org/markets-and-operations/real-time--market-data/)
- [BaseCollector Framework](../../infrastructure/collection_framework.py)
- [Redis Documentation](https://redis.io/docs/)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)

## License

Copyright © 2025. All rights reserved.
