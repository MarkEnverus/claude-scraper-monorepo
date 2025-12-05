# MISO Public API Ex-Ante LMP Scraper

Collects real-time forward-looking Locational Marginal Price snapshots from MISO's Public API for intraday trading and short-term price forecasting.

## Overview

**Data Source**: MISO (Midcontinent Independent System Operator)
**API Endpoint**: `https://public-api.misoenergy.org/api/MarketPricing/GetExAnteLmp`
**Authentication**: None (public API)
**Update Frequency**: Every 5 minutes
**Collection Pattern**: Snapshot-based (real-time forward-looking prices)

## What is Public API Ex-Ante LMP?

Ex-Ante LMP represents forecasted Locational Marginal Prices for the next 4 hours, providing real-time forward-looking price information for market hubs. This data is:

- Updated every 5 minutes
- Includes total LMP and components (energy, congestion, losses)
- Available for multiple market hubs (CINERGY, MINN, AECI, ARKANSAS, ILLINOIS, etc.)
- Used for real-time price discovery and short-term trading decisions

### Key Differences from Other LMP Sources

| Source | Type | Time Window | Use Case |
|--------|------|-------------|----------|
| **Public Ex-Ante LMP** (this scraper) | Real-time snapshot | Next 4 hours | Intraday trading, real-time monitoring |
| Day-Ahead Ex-Ante LMP | Historical forecasts | Day-ahead (24 hours) | Pre-market planning, daily forecasts |
| Real-Time Ex-Post LMP | Actual prices | Past (5-min intervals) | Settlement, actual market outcomes |

## Data Structure

### API Response Format

```json
{
  "timestamp": "2025-12-05T14:30:00Z",
  "updateInterval": "5m",
  "hubs": [
    {
      "name": "CINERGY",
      "lmp": 25.50,
      "components": {
        "energy": 24.80,
        "congestion": 0.45,
        "losses": 0.25
      }
    }
  ]
}
```

### Field Descriptions

- **timestamp**: ISO 8601 datetime in UTC representing the moment of this price snapshot
- **updateInterval**: Update frequency (fixed "5m")
- **hubs**: Array of hub objects with LMP data
  - **name**: Market hub identifier (e.g., "CINERGY", "MINN", "AECI")
  - **lmp**: Total Locational Marginal Price in $/MWh
  - **components**: Price breakdown
    - **energy**: Pure energy generation cost component
    - **congestion**: Transmission constraint cost component
    - **losses**: Transmission energy loss cost component

### LMP Components Relationship

```
LMP = Energy + Congestion + Losses
```

All price components can be positive or negative depending on market conditions.

## Query Parameters

All parameters are optional:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `startTime` | ISO-8601 datetime | Start of time window | `2025-12-05T14:30:00Z` |
| `endTime` | ISO-8601 datetime | End of time window (max 4 hours from start) | `2025-12-05T18:30:00Z` |
| `hub` | String | Filter by specific hub | `CINERGY`, `MINN`, `AECI` |

## Installation

### Prerequisites

1. **Redis**: Required for hash-based deduplication
   ```bash
   # Install Redis (macOS)
   brew install redis
   redis-server

   # Or using Docker
   docker run -d -p 6379:6379 redis:latest
   ```

2. **AWS Credentials**: For S3 storage
   ```bash
   aws configure
   # Or use AWS_PROFILE environment variable
   ```

3. **Python Dependencies**: Installed automatically via project dependencies

### Environment Variables

```bash
# Required
export S3_BUCKET=your-bucket-name

# Optional
export AWS_PROFILE=your-aws-profile
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB=0
export KAFKA_CONNECTION_STRING=kafka://...  # For notifications
```

## Usage

### Basic Collection (Current Snapshot)

```bash
python scraper_miso_public_exante_lmp.py --s3-bucket my-bucket
```

### Collection with Time Window

```bash
python scraper_miso_public_exante_lmp.py \
  --s3-bucket my-bucket \
  --start-time "2025-12-05T14:00:00Z" \
  --end-time "2025-12-05T18:00:00Z"
```

### Filter by Specific Hub

```bash
python scraper_miso_public_exante_lmp.py \
  --s3-bucket my-bucket \
  --hub CINERGY
```

### Development/Testing with LocalStack

```bash
python scraper_miso_public_exante_lmp.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack \
  --log-level DEBUG
```

### Production Scheduling

For continuous monitoring, schedule the scraper to run every 5 minutes:

```bash
# Using cron
*/5 * * * * /path/to/venv/bin/python /path/to/scraper_miso_public_exante_lmp.py --s3-bucket prod-bucket
```

## Storage

### S3 Path Structure

```
s3://{bucket}/sourcing/miso_public_exante_lmp/{YYYY}/{MM}/{DD}/public_exante_lmp_{YYYYMMDD_HHMM}.json.gz
```

### Example Paths

```
s3://my-bucket/sourcing/miso_public_exante_lmp/2025/12/05/public_exante_lmp_20251205_1430.json.gz
s3://my-bucket/sourcing/miso_public_exante_lmp/2025/12/05/public_exante_lmp_20251205_1435_hub_CINERGY.json.gz
```

### Storage Features

- **Compression**: gzip compression reduces storage costs by ~75%
- **Deduplication**: Redis hash-based deduplication prevents duplicate storage
- **Partitioning**: Date-based partitioning for efficient queries
- **Retention**: Follow S3 lifecycle policies for data retention

## Validation

The scraper performs comprehensive validation on collected data:

### Structure Validation

- Required fields: `timestamp`, `updateInterval`, `hubs`
- Timestamp must be valid ISO 8601 format
- `updateInterval` must be "5m"
- `hubs` must be an array (can be empty if no data available)

### Hub Validation

- Each hub must have: `name`, `lmp`, `components`
- Hub name must be non-empty string
- LMP must be numeric
- Components must include: `energy`, `congestion`, `losses`
- All components must be numeric

### LMP Arithmetic Validation

```python
# Validates: lmp = energy + congestion + losses
# Tolerance: ±0.01 (allows for rounding)
calculated_lmp = energy + congestion + losses
assert abs(calculated_lmp - lmp) <= 0.01
```

### Price Range Validation

- All price components can be positive or negative
- Negative values are valid (e.g., negative congestion during low demand)
- No artificial bounds on price ranges

## Testing

### Run All Tests

```bash
pytest sourcing/scraping/miso/public_exante_lmp/tests/ -v
```

### Run Specific Test Classes

```bash
# Test candidate generation
pytest sourcing/scraping/miso/public_exante_lmp/tests/test_scraper_miso_public_exante_lmp.py::TestCandidateGeneration -v

# Test content collection
pytest sourcing/scraping/miso/public_exante_lmp/tests/test_scraper_miso_public_exante_lmp.py::TestContentCollection -v

# Test validation logic
pytest sourcing/scraping/miso/public_exante_lmp/tests/test_scraper_miso_public_exante_lmp.py::TestContentValidation -v
```

### Test Coverage

The test suite includes:

- Candidate generation with and without query parameters
- API interaction and HTTP error handling (400, 404, timeout)
- Comprehensive validation rules
  - Missing fields
  - Invalid data types
  - LMP arithmetic validation
  - Negative price components
  - Empty hubs array
- Integration with BaseCollector framework

## Monitoring

### Key Metrics

1. **Collection Success Rate**: Should be >95%
2. **Duplicate Rate**: Normal <5% (depends on scheduling frequency)
3. **Validation Failure Rate**: Should be <1%
4. **API Response Time**: Normal <5 seconds
5. **Data Freshness**: Alert if no new data for >30 minutes

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| 404 errors | Outside market hours or no data available | Normal - skip and retry later |
| 400 errors | Invalid query parameters | Check startTime/endTime format and 4-hour window |
| Validation failures | API schema change | Update validation logic |
| High duplicate rate | Scraper running too frequently | Reduce to 5-minute intervals |
| Redis connection errors | Redis not running | Start Redis service |
| S3 upload errors | AWS credentials or permissions | Check AWS_PROFILE and IAM permissions |

## Use Cases

### 1. Intraday Trading

```python
# Collect current forward prices every 5 minutes
# Analyze price trends for next 4 hours
# Identify arbitrage opportunities between hubs
```

### 2. Real-Time Price Monitoring

```python
# Monitor specific hub (e.g., CINERGY)
# Alert on price spikes or unusual congestion
# Track price volatility in real-time
```

### 3. Short-Term Forecasting

```python
# Collect historical snapshots
# Build models to predict next-hour prices
# Validate forecast accuracy against actual prices
```

### 4. Market Analysis

```python
# Compare Ex-Ante forecasts with Ex-Post actual prices
# Analyze forecast accuracy by hub
# Identify systematic biases in price forecasts
```

## Integration with Downstream Systems

### Kafka Notifications

When `--kafka-connection-string` is provided, the scraper publishes notifications for each collected file:

```json
{
  "event": "file_collected",
  "dgroup": "miso_public_exante_lmp",
  "file_path": "sourcing/miso_public_exante_lmp/2025/12/05/public_exante_lmp_20251205_1430.json.gz",
  "timestamp": "2025-12-05T14:30:00Z",
  "metadata": {
    "data_type": "public_exante_lmp",
    "source": "miso",
    "hub_count": 5
  }
}
```

### Time Series Database Integration

Example: Store to InfluxDB for time-series analysis

```python
# Pseudocode
for hub in data['hubs']:
    influxdb.write_point(
        measurement="miso_exante_lmp",
        tags={"hub": hub['name']},
        fields={
            "lmp": hub['lmp'],
            "energy": hub['components']['energy'],
            "congestion": hub['components']['congestion'],
            "losses": hub['components']['losses']
        },
        timestamp=data['timestamp']
    )
```

## Related Scrapers

- **MISO Day-Ahead Ex-Ante LMP** (`da_exante_lmp`): Historical daily forecasts (CSV format)
- **MISO Day-Ahead Ex-Ante LMP API** (`da_exante_lmp_api`): Historical daily forecasts (API format)
- **MISO Real-Time Ex-Post LMP** (`rt_expost_lmp`): Actual real-time prices (5-min intervals)
- **MISO Snapshot** (`snapshot`): General grid status metrics

## Troubleshooting

### Debug Mode

```bash
python scraper_miso_public_exante_lmp.py \
  --s3-bucket test-bucket \
  --log-level DEBUG
```

### Verify Redis Connection

```bash
redis-cli ping
# Should return: PONG
```

### Test API Access

```bash
curl "https://public-api.misoenergy.org/api/MarketPricing/GetExAnteLmp"
```

### Check S3 Permissions

```bash
aws s3 ls s3://your-bucket/sourcing/miso_public_exante_lmp/
```

## Support

For issues or questions:
1. Check logs for specific error messages
2. Verify all prerequisites (Redis, AWS credentials)
3. Test API endpoint directly with curl
4. Review validation logic for schema changes

## Version History

- **1.0.0** (2025-12-05): Initial release
  - Snapshot-based collection with optional query parameters
  - Comprehensive LMP arithmetic validation
  - Redis deduplication and S3 storage
  - Full test coverage
