# MISO Wind Forecast Scraper

Collects real-time wind power forecast data from MISO's public API.

## Overview

**Data Provider:** MISO (Midcontinent Independent System Operator)
**Data Source:** Wind Forecast
**Collection Method:** HTTP API
**API Endpoint:** https://public-api.misoenergy.org/api/WindSolar/getwindforecast

**Infrastructure Version:** 1.3.0
**Last Updated:** 2025-12-02

## Features

✅ **HTTP Collection** - Uses BaseCollector framework for reliable API data collection
✅ **Redis Deduplication** - Hash-based duplicate detection prevents redundant storage
✅ **S3 Storage** - Automatic gzip compression and date-partitioned storage
✅ **Kafka Notifications** - Optional downstream processing notifications
✅ **Comprehensive Testing** - Full test suite with fixtures and mocks
✅ **Error Handling** - Robust validation and error recovery

## Data Structure

The API returns wind forecast data in the following format:

```json
{
  "Forecast": [
    {
      "DateTimeEST": "2025-12-02T10:00:00",
      "Value": 12500.5
    },
    {
      "DateTimeEST": "2025-12-02T11:00:00",
      "Value": 13200.3
    }
  ]
}
```

**Fields:**
- `DateTimeEST` - Forecast timestamp in Eastern Time
- `Value` - Wind power forecast in MW

## Installation

### Prerequisites

```bash
# Python 3.10+ required
python --version

# Install dependencies
pip install -r requirements.txt
```

### Environment Setup

Set up required environment variables:

```bash
# Required
export S3_BUCKET="your-bucket-name"
export AWS_PROFILE="localstack"  # or your AWS profile

# Redis (defaults provided)
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
export REDIS_DB="0"

# Optional: Kafka notifications
export KAFKA_CONNECTION_STRING="kafka://localhost:9092"
```

## Usage

### Basic Collection

```bash
python scraper_miso_wind_forecast.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack \
  --environment dev
```

### With Kafka Notifications

```bash
python scraper_miso_wind_forecast.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack \
  --environment dev \
  --kafka-connection-string kafka://localhost:9092
```

### Command-Line Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `--s3-bucket` | `S3_BUCKET` | *(required)* | S3 bucket for storage |
| `--aws-profile` | `AWS_PROFILE` | None | AWS profile (for LocalStack or AWS) |
| `--environment` | - | `dev` | Environment: dev/staging/prod |
| `--redis-host` | `REDIS_HOST` | `localhost` | Redis server host |
| `--redis-port` | `REDIS_PORT` | `6379` | Redis server port |
| `--redis-db` | `REDIS_DB` | `0` | Redis database number |
| `--kafka-connection-string` | `KAFKA_CONNECTION_STRING` | None | Kafka connection (optional) |
| `--log-level` | - | `INFO` | Logging: DEBUG/INFO/WARNING/ERROR |

## Storage Structure

Data is stored in S3 with date partitioning:

```
s3://{bucket}/sourcing/miso_wind_forecast/
  └── year=2025/
      └── month=12/
          └── day=02/
              └── wind_forecast_20251202_1400.json.gz
```

**File Naming:** `wind_forecast_{YYYYMMDD}_{HHMM}.json.gz`

## Testing

### Run All Tests

```bash
pytest sourcing/scraping/miso/wind_forecast/tests/ -v
```

### Run Specific Test Categories

```bash
# Candidate generation
pytest sourcing/scraping/miso/wind_forecast/tests/ -k "TestCandidateGeneration" -v

# Content validation
pytest sourcing/scraping/miso/wind_forecast/tests/ -k "TestContentValidation" -v

# Kafka integration
pytest sourcing/scraping/miso/wind_forecast/tests/ -k "TestKafkaIntegration" -v

# End-to-end
pytest sourcing/scraping/miso/wind_forecast/tests/ -k "TestEndToEndCollection" -v
```

### Test Coverage

The test suite includes:
- ✅ Candidate generation logic
- ✅ HTTP content collection with error handling
- ✅ JSON validation (structure, required fields)
- ✅ S3 upload with gzip compression
- ✅ Hash deduplication
- ✅ Kafka notification publishing
- ✅ Error handling and recovery
- ✅ End-to-end integration tests

## Development

### Project Structure

```
sourcing/scraping/miso/wind_forecast/
├── __init__.py
├── README.md
├── scraper_miso_wind_forecast.py
└── tests/
    ├── __init__.py
    ├── fixtures/
    │   └── sample_wind_forecast.json
    └── test_scraper_miso_wind_forecast.py
```

### Adding New Features

1. Update the scraper code in `scraper_miso_wind_forecast.py`
2. Add tests in `tests/test_scraper_miso_wind_forecast.py`
3. Update this README
4. Update `LAST_UPDATED` timestamp in docstring
5. Run tests to verify changes

### Debugging

Enable debug logging:

```bash
python scraper_miso_wind_forecast.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack \
  --log-level DEBUG
```

## Collection Behavior

### Deduplication

- Content is hashed using SHA-256
- Hash is checked against Redis before upload
- Duplicate content is skipped (not re-uploaded)
- Hash TTL: 365 days (configurable)

### Error Handling

The scraper handles:
- ✅ Network timeouts and connection errors
- ✅ HTTP 4xx/5xx errors
- ✅ Invalid JSON responses
- ✅ Missing required fields
- ✅ S3 upload failures
- ✅ Kafka notification failures (non-blocking)

### Validation

Content is validated for:
- ✅ Valid JSON structure
- ✅ Presence of `Forecast` array
- ✅ Non-empty forecast data
- ✅ Required fields: `DateTimeEST`, `Value`

## Monitoring

### Success Indicators

```
INFO - Generated 1 candidates
INFO - Successfully fetched 1234 bytes
INFO - Content validation passed (72 forecast entries)
INFO - Successfully collected
INFO - Collection completed: {...}
```

### Error Indicators

```
WARNING - Content validation failed
ERROR - Failed to fetch wind forecast: Connection timeout
ERROR - Collection failed
```

### Collection Results

```json
{
  "total_candidates": 1,
  "collected": 1,
  "skipped_duplicate": 0,
  "failed": 0,
  "errors": []
}
```

## API Reference

### MISO Wind Forecast API

**Endpoint:** `GET https://public-api.misoenergy.org/api/WindSolar/getwindforecast`

**Response Format:** JSON
**Authentication:** None (public API)
**Rate Limits:** Not specified
**Update Frequency:** Real-time (5-minute intervals)

**Response Example:**
```json
{
  "Forecast": [
    {
      "DateTimeEST": "2025-12-02T10:00:00",
      "Value": 12500.5
    }
  ]
}
```

## Troubleshooting

### Common Issues

**Issue:** `Failed to connect to Redis`
```bash
# Check Redis is running
redis-cli ping

# Should return: PONG
```

**Issue:** `S3 upload failed`
```bash
# Check AWS credentials
aws s3 ls s3://your-bucket --profile localstack

# Verify bucket exists
aws s3 mb s3://scraper-testing --profile localstack
```

**Issue:** `Content validation failed`
- Check API response format hasn't changed
- Verify network connectivity to MISO API
- Check API endpoint is accessible

**Issue:** `Kafka notification failed`
- Kafka failures are non-blocking
- Collection continues even if Kafka unavailable
- Check Kafka connection string format

## Changelog

### 1.3.0 (2025-12-02)
- ✅ Added infrastructure version tracking
- ✅ Added Kafka notification support
- ✅ Comprehensive test suite with fixtures
- ✅ Updated to monorepo layout: `scraping/miso/wind_forecast/`
- ✅ Enhanced documentation

### 1.0.0 (Initial Release)
- ✅ HTTP collection from MISO API
- ✅ Redis hash deduplication
- ✅ S3 storage with date partitioning
- ✅ Content validation

## License

Internal use only.

## Contact

For issues or questions, contact the data engineering team.
