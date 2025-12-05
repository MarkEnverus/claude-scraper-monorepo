# MISO Regional Directional Transfer Scraper

Collects real-time North-South transmission corridor flow data from MISO's public API.

## Overview

This scraper monitors inter-regional power transmission dynamics by collecting:
- North-South transmission corridor flow limits
- Raw power transfer measurements (MW)
- Unidirectional flow measurements
- Directional capacity constraints

## Data Source

**API Endpoint**: `https://public-api.misoenergy.org/api/RegionalDirectionalTransfer`

**Authentication**: None required (public API)

**Update Frequency**: Real-time (5-minute intervals)

**Data Format**: JSON

## Response Structure

```json
{
  "RefId": "05-Dec-2025 - Interval 10:35 EST",
  "Interval": [
    {
      "instantEST": "2025-12-05 10:35:00 AM",
      "NORTH_SOUTH_LIMIT": "-3000",
      "SOUTH_NORTH_LIMIT": "2500",
      "RAW_MW": "1234.56",
      "UDSFLOW_MW": "1234.56"
    }
  ]
}
```

### Field Descriptions

- **RefId**: Human-readable timestamp reference
- **instantEST**: Precise measurement timestamp (Eastern Standard Time)
- **NORTH_SOUTH_LIMIT**: Maximum power transfer capacity from North to South (MW, typically negative)
- **SOUTH_NORTH_LIMIT**: Maximum power transfer capacity from South to North (MW, typically positive)
- **RAW_MW**: Raw power transfer measurement with directional information
- **UDSFLOW_MW**: Unidirectional flow measurement (absolute value)

## Installation

### Prerequisites

- Python 3.9+
- Redis (for deduplication)
- AWS credentials (for S3 storage)
- LocalStack (for local development)

### Dependencies

```bash
pip install boto3 click redis requests
```

## Usage

### Basic Usage

```bash
python scraper_miso_regional_directional_transfer.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack
```

### Environment Variables

```bash
# Required
export S3_BUCKET=scraper-testing

# Optional
export AWS_PROFILE=localstack
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB=0
export KAFKA_CONNECTION_STRING=user:pass@kafka-host:9092
```

### Command Line Options

```
--s3-bucket              S3 bucket name for storage (required)
--aws-profile            AWS profile to use (optional)
--environment            Environment: dev/staging/prod (default: dev)
--redis-host             Redis host (default: localhost)
--redis-port             Redis port (default: 6379)
--redis-db               Redis database number (default: 0)
--log-level              Logging level (default: INFO)
--kafka-connection-string Kafka connection for notifications (optional)
```

## Architecture

### Collection Flow

1. **Generate Candidate**: Creates single candidate for current 5-minute interval
2. **Fetch Content**: HTTP GET request to MISO API
3. **Validate Content**: Validates JSON structure and field ranges
4. **Check Duplicate**: Uses Redis hash registry for deduplication
5. **Store to S3**: Uploads to S3 with date partitioning and gzip compression
6. **Notify Kafka**: Sends notification for downstream processing (optional)

### Data Storage

Files are stored in S3 with date partitioning:

```
s3://{bucket}/sourcing/
  dgroup=miso_regional_directional_transfer/
    year=YYYY/
      month=MM/
        day=DD/
          regional_directional_transfer_YYYYMMDD_HHMM.json.gz
```

### Deduplication

Content hashing using SHA-256 with Redis-based registry:
- Hash key: `{dgroup}:{content_hash}`
- TTL: 7 days
- Prevents duplicate storage of identical content

## Validation Rules

The scraper validates:

1. **Structure Validation**:
   - Response is a dictionary
   - Contains required fields: RefId, Interval
   - Interval is a non-empty array

2. **Field Validation**:
   - All required fields present: instantEST, NORTH_SOUTH_LIMIT, SOUTH_NORTH_LIMIT, RAW_MW, UDSFLOW_MW
   - All numeric fields are valid numbers

3. **Range Validation**:
   - NORTH_SOUTH_LIMIT: -3500 to 0 MW
   - SOUTH_NORTH_LIMIT: 0 to 3000 MW
   - UDSFLOW_MW: Non-negative

## Testing

### Run Tests

```bash
# Run all tests
pytest sourcing/scraping/miso/regional_directional_transfer/tests/ -v

# Run specific test class
pytest sourcing/scraping/miso/regional_directional_transfer/tests/test_scraper_miso_regional_directional_transfer.py::TestContentValidation -v

# Run with coverage
pytest sourcing/scraping/miso/regional_directional_transfer/tests/ --cov=sourcing.scraping.miso.regional_directional_transfer --cov-report=html
```

### Test Coverage

Tests include:
- Candidate generation
- Content collection (success, timeout, HTTP errors)
- Content validation (valid/invalid data)
- Integration tests (full flow, duplicate detection)

## Monitoring

### Collection Metrics

The scraper reports:
- Total candidates generated
- Successfully collected files
- Skipped duplicates
- Failed collections

### Logging

Logs are structured in JSON format with:
- Timestamp
- Log level
- Message
- Context (dgroup, candidate identifier, etc.)

## Error Handling

- **HTTP Errors**: Retries with exponential backoff
- **Validation Failures**: Logged but not retried
- **Redis Failures**: Fails fast (deduplication is critical)
- **S3 Failures**: Retries with boto3 default retry policy

## Business Value

- **Grid Stability**: Monitor transmission corridor constraints in real-time
- **Transmission Planning**: Analyze North-South power flow patterns
- **Market Intelligence**: Understand regional power transfer capabilities
- **Risk Management**: Assess potential transmission congestion risks

## Support

For issues or questions:
1. Check logs for error messages
2. Verify Redis connectivity
3. Verify AWS credentials and S3 bucket access
4. Review API endpoint accessibility

## Version Information

- **Infrastructure Version**: 1.3.0
- **Last Updated**: 2025-12-05
- **Collection Framework**: BaseCollector v1.3.0

## Related Scrapers

- `scraper_miso_snapshot.py`: Grid status snapshot data
- `scraper_miso_rt_exante_lmp.py`: Real-time LMP pricing
- `scraper_miso_fuel_mix.py`: Fuel mix generation data
