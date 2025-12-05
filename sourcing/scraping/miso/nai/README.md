# MISO Net Actual Interchange (NAI) Scraper

Collects real-time net actual interchange data from MISO's public API.

## Overview

This scraper collects snapshot data showing power flows across interconnection points between MISO and neighboring control areas. The data is updated approximately every 5 minutes and provides near real-time visibility into actual power interchange.

**API Endpoint**: `https://public-api.misoenergy.org/api/Interchange/GetNai`

## What is Net Actual Interchange?

Net Actual Interchange (NAI) represents the actual power flows (in MW) across tie lines between MISO and adjacent control areas:

- **Positive values**: Power flowing INTO MISO from the neighboring area
- **Negative values**: Power flowing OUT OF MISO to the neighboring area

## Data Structure

The API returns JSON with the following structure:

```json
{
  "data": [
    {
      "RefId": "MISO_SPP_1",
      "TieFlowName": "MISO - Southwest Power Pool",
      "TieFlowValue": 1234.56,
      "Timestamp": "2025-12-05T14:30:00Z"
    }
  ]
}
```

### Fields

- **RefId**: Unique identifier for the tie flow (e.g., "MISO_SPP_1")
- **TieFlowName**: Descriptive name of the interconnection
- **TieFlowValue**: Power flow in MW (can be positive, negative, or zero)
- **Timestamp**: ISO 8601 timestamp in UTC

### Typical Tie Flows

The API typically returns 5-15 tie flows including connections to:

- Southwest Power Pool (SPP)
- PJM Interconnection
- Tennessee Valley Authority (TVA)
- Louisville Gas and Electric (LGEE)
- Associated Electric Cooperative (AECI)
- Other neighboring control areas

## Usage

### Basic Execution

```bash
python scraper_miso_nai.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack
```

### Production Execution

```bash
python scraper_miso_nai.py \
  --s3-bucket production-data \
  --environment prod \
  --redis-host redis.production.com \
  --redis-port 6379 \
  --kafka-connection-string kafka://broker:9092
```

### All Options

```bash
python scraper_miso_nai.py \
  --s3-bucket BUCKET_NAME \
  --aws-profile AWS_PROFILE \
  --environment {dev|staging|prod} \
  --redis-host REDIS_HOST \
  --redis-port REDIS_PORT \
  --redis-db REDIS_DB \
  --log-level {DEBUG|INFO|WARNING|ERROR} \
  --kafka-connection-string KAFKA_URL
```

## Environment Variables

The scraper supports these environment variables as alternatives to CLI options:

- `S3_BUCKET`: S3 bucket name (required)
- `AWS_PROFILE`: AWS profile to use
- `REDIS_HOST`: Redis hostname (default: localhost)
- `REDIS_PORT`: Redis port (default: 6379)
- `REDIS_DB`: Redis database number (default: 0)
- `KAFKA_CONNECTION_STRING`: Kafka connection string (optional)

## Storage

Data is stored in S3 with the following structure:

```
s3://{bucket}/sourcing/miso_net_actual_interchange/{date}/nai_{timestamp}.json.gz
```

Example:
```
s3://scraper-testing/sourcing/miso_net_actual_interchange/2025-12-05/nai_20251205_1430.json.gz
```

## Deduplication

The scraper uses Redis-based content hashing to avoid storing duplicate data. If the same NAI data is fetched multiple times (e.g., no updates between collection runs), only the first occurrence is stored to S3.

## Validation

The scraper performs comprehensive validation:

1. Response must be valid JSON
2. Response must contain a 'data' key with a list
3. At least 1 tie flow must be present
4. Each tie flow must have: RefId, TieFlowName, TieFlowValue, Timestamp
5. TieFlowValue must be numeric
6. TieFlowName must be a non-empty string
7. Timestamp must be valid ISO 8601 format

## Testing

### Run All Tests

```bash
pytest sourcing/scraping/miso/nai/tests/ -v
```

### Run Specific Test Classes

```bash
# Test candidate generation
pytest sourcing/scraping/miso/nai/tests/test_scraper_miso_nai.py::TestCandidateGeneration -v

# Test content collection
pytest sourcing/scraping/miso/nai/tests/test_scraper_miso_nai.py::TestContentCollection -v

# Test validation
pytest sourcing/scraping/miso/nai/tests/test_scraper_miso_nai.py::TestContentValidation -v

# Test integration
pytest sourcing/scraping/miso/nai/tests/test_scraper_miso_nai.py::TestIntegration -v
```

### Test Coverage

The test suite includes:

- ✅ Candidate generation (4 tests)
- ✅ Content collection (3 tests)
- ✅ Content validation (11 tests)
- ✅ Integration tests (2 tests)

**Total: 21 tests**

## Scheduling

### Recommended Schedule

- **Production**: Run every 5 minutes (matches MISO update frequency)
- **Development**: Run every 15 minutes
- **Testing**: Manual execution as needed

### Cron Examples

Production (every 5 minutes):
```cron
*/5 * * * * /path/to/venv/bin/python /path/to/scraper_miso_nai.py --s3-bucket prod-data --environment prod
```

Development (every 15 minutes):
```cron
*/15 * * * * /path/to/venv/bin/python /path/to/scraper_miso_nai.py --s3-bucket dev-data --environment dev
```

## Monitoring

### Key Metrics

1. **Collection Rate**: Should succeed ~288 times/day (every 5 minutes)
2. **Duplicate Rate**: Normal ~5-10% (data doesn't always change)
3. **Validation Failure Rate**: Should be <1%
4. **API Response Time**: Normal <5 seconds

### Alerting Thresholds

- Alert if no successful collection in 30 minutes
- Alert if validation failure rate exceeds 5%
- Alert if API response time exceeds 30 seconds consistently
- Alert if duplicate rate exceeds 50% (may indicate data staleness)

## Expected Data Volume

- **Update Frequency**: Every 5 minutes
- **Tie Flows per Update**: 5-15 typically
- **Data Size (uncompressed)**: ~500-1500 bytes per update
- **Daily Volume**: ~288 updates = ~150-450 KB/day (uncompressed)
- **Monthly Volume**: ~4.5-13 MB/month (uncompressed)
- **With gzip compression**: ~30-40% of uncompressed size

## Dependencies

- Python 3.8+
- boto3 (AWS SDK)
- redis (for deduplication)
- requests (HTTP client)
- click (CLI framework)

## Error Handling

The scraper handles these error scenarios:

1. **Network Timeouts**: 30-second timeout on HTTP requests
2. **HTTP Errors**: 4xx/5xx status codes handled gracefully
3. **Invalid JSON**: Validation fails, error logged
4. **Missing Fields**: Validation fails, error logged
5. **Redis Connection**: Fails fast with clear error message
6. **S3 Upload Errors**: Retried with exponential backoff (via boto3)

## Infrastructure

This scraper uses the standardized collection framework:

- **BaseCollector**: Base class providing common functionality
- **HashRegistry**: Redis-based content deduplication
- **KafkaProducer**: Optional notifications for downstream systems
- **S3 Storage**: Compressed JSON with date partitioning

### Infrastructure Version

- **Version**: 1.3.0
- **Last Updated**: 2025-12-05

## Troubleshooting

### No Data Collected

1. Check MISO API status: `curl https://public-api.misoenergy.org/api/Interchange/GetNai`
2. Verify network connectivity
3. Check logs for validation errors

### High Duplicate Rate

1. Verify MISO is updating data (check Timestamp field)
2. Check if collection frequency matches data update frequency
3. Review Redis deduplication settings

### S3 Upload Failures

1. Verify AWS credentials are configured
2. Check S3 bucket permissions
3. Verify bucket name is correct
4. Check AWS_PROFILE setting if using LocalStack

### Redis Connection Errors

1. Verify Redis is running: `redis-cli ping`
2. Check REDIS_HOST and REDIS_PORT settings
3. Verify network connectivity to Redis

## Related Documentation

- [MISO Public API Documentation](https://www.misoenergy.org/markets-and-operations/real-time--market-data/)
- [Collection Framework](../../infrastructure/collection_framework.py)
- [Hash Registry](../../infrastructure/hash_registry.py)
- [Configuration File](.scraper-dev.md)

## Support

For issues or questions:

1. Check logs for error messages
2. Review validation failures in detail
3. Verify API endpoint is accessible
4. Check infrastructure dependencies (Redis, S3, AWS credentials)
