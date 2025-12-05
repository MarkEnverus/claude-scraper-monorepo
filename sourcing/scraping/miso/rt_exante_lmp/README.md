# MISO Real-Time Ex-Ante LMP Scraper

## Overview

This scraper collects Real-Time Ex-Ante (forecasted) Locational Marginal Pricing data from MISO's Pricing API. Ex-Ante LMP represents forecasted prices for the real-time energy market across MISO's commercial pricing nodes (CPNodes).

**API Endpoint**: `https://apim.misoenergy.org/pricing/v1/real-time/{date}/lmp-exante`

## Key Features

- HTTP REST API collection using BaseCollector framework
- Configurable time resolution: hourly (24 intervals) or 5-minute (288 intervals)
- Automatic pagination handling for high-volume data
- API key authentication via `Ocp-Apim-Subscription-Key` header
- Redis-based hash deduplication
- S3 storage with date partitioning and gzip compression
- Kafka notifications for downstream processing
- Comprehensive error handling and validation
- LMP arithmetic validation (LMP = MEC + MCC + MLC)

## Data Characteristics

### Publication Schedule
- **First Available**: 7:00 AM EST on operating day
- **Updates**: Continuously updated throughout operating day (near real-time)
- **Forecast Nature**: This is Ex-Ante (forecasted) data, not actual/settled prices

### Time Resolutions

#### Hourly Resolution
- **Intervals**: 24 per day (hours 1-24)
- **Volume**: ~72,000-120,000 records/day (3,000-5,000 nodes × 24 intervals)
- **Use Case**: Standard forecasting, lower data volume

#### 5-Minute Resolution
- **Intervals**: 288 per day (every 5 minutes)
- **Volume**: ~300,000-400,000 records/day (3,000-5,000 nodes × 288 intervals)
- **Use Case**: High-frequency trading, detailed grid operations

### Data Format

Each record contains:
- **node**: CPNode identifier (e.g., "ALTW.WELLS1", "AMEREN.ILLINOIS")
- **lmp**: Locational Marginal Price (forecasted)
- **mcc**: Marginal Congestion Cost component (forecasted)
- **mec**: Marginal Energy Cost component (forecasted)
- **mlc**: Marginal Loss Cost component (forecasted)
- **interval**: Hour (1-24) or time (HH:MM) depending on resolution
- **timeInterval**: Object with resolution, start/end times, and date

## Prerequisites

### Required Services
- **Redis**: For hash-based deduplication
- **AWS S3**: For data storage
- **MISO API Key**: From MISO's developer portal

### Environment Variables
```bash
export MISO_API_KEY=your_subscription_key_here
export S3_BUCKET=your-s3-bucket-name
export AWS_PROFILE=your-aws-profile  # Optional
export REDIS_HOST=localhost          # Default: localhost
export REDIS_PORT=6379               # Default: 6379
```

## Installation

```bash
# Install dependencies
pip install boto3 click redis requests

# Verify infrastructure files exist
ls -l sourcing/infrastructure/collection_framework.py
ls -l sourcing/infrastructure/logging_json.py
```

## Usage

### Basic Usage - Hourly Resolution

```bash
python scraper_miso_rt_exante_lmp.py \
  --api-key YOUR_API_KEY \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --time-resolution hourly
```

### Using Environment Variables

```bash
export MISO_API_KEY=your_key
export S3_BUCKET=your-bucket

python scraper_miso_rt_exante_lmp.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31
```

### 5-Minute Resolution (High Volume)

```bash
python scraper_miso_rt_exante_lmp.py \
  --api-key YOUR_API_KEY \
  --start-date 2025-01-20 \
  --end-date 2025-01-20 \
  --time-resolution 5min
```

### Force Re-download

```bash
python scraper_miso_rt_exante_lmp.py \
  --api-key YOUR_API_KEY \
  --start-date 2025-01-01 \
  --end-date 2025-01-02 \
  --force
```

### Debug Logging

```bash
python scraper_miso_rt_exante_lmp.py \
  --api-key YOUR_API_KEY \
  --start-date 2025-01-20 \
  --end-date 2025-01-20 \
  --log-level DEBUG
```

## Command-Line Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--api-key` | Yes* | `$MISO_API_KEY` | MISO API subscription key |
| `--start-date` | Yes | - | Start date (YYYY-MM-DD) |
| `--end-date` | Yes | - | End date (YYYY-MM-DD) |
| `--time-resolution` | No | `hourly` | Time resolution: `5min` or `hourly` |
| `--s3-bucket` | No* | `$S3_BUCKET` | S3 bucket name |
| `--aws-profile` | No | `$AWS_PROFILE` | AWS profile name |
| `--redis-host` | No | `localhost` | Redis host |
| `--redis-port` | No | `6379` | Redis port |
| `--redis-db` | No | `0` | Redis database number |
| `--environment` | No | `dev` | Environment: `dev`, `staging`, `prod` |
| `--force` | No | `False` | Force re-download existing files |
| `--skip-hash-check` | No | `False` | Skip Redis deduplication |
| `--log-level` | No | `INFO` | Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |

*Can be set via environment variable

## Output

### S3 Storage Structure

```
s3://your-bucket/sourcing/miso_rt_exante_lmp/
├── year=2025/
│   ├── month=01/
│   │   ├── day=01/
│   │   │   └── rt_exante_lmp_hourly_20250101.json.gz
│   │   ├── day=02/
│   │   │   └── rt_exante_lmp_hourly_20250102.json.gz
│   │   └── day=20/
│   │       └── rt_exante_lmp_5min_20250120.json.gz
```

### File Format

Each file contains a JSON object:

```json
{
  "data": [
    {
      "node": "ALTW.WELLS1",
      "lmp": 22.1,
      "mcc": 0.03,
      "mec": 21.34,
      "mlc": 0.73,
      "interval": "1",
      "timeInterval": {
        "resolution": "hourly",
        "start": "2025-01-01 00:00:00.000",
        "end": "2025-01-01 01:00:00.000",
        "value": "2025-01-01"
      }
    }
  ],
  "total_records": 72000,
  "total_pages": 150,
  "time_resolution": "hourly",
  "metadata": {
    "data_type": "rt_exante_lmp",
    "source": "miso",
    "date": "2025-01-01",
    "market_type": "real_time_energy_exante",
    "forecast": true
  }
}
```

## Testing

### Run All Tests

```bash
pytest sourcing/scraping/miso/rt_exante_lmp/tests/ -v
```

### Run Specific Test Class

```bash
pytest sourcing/scraping/miso/rt_exante_lmp/tests/test_scraper_miso_rt_exante_lmp.py::TestCandidateGeneration -v
```

### Run with Coverage

```bash
pytest sourcing/scraping/miso/rt_exante_lmp/tests/ --cov=sourcing.scraping.miso.rt_exante_lmp --cov-report=html
```

## Error Handling

### Common Errors

| Error Code | Meaning | Action |
|------------|---------|--------|
| 400 | Bad Request | Check date format (YYYY-MM-DD) and parameters |
| 401 | Unauthorized | Verify API key is correct |
| 404 | Not Found | Data may not be available yet (forecast not published) |
| 429 | Rate Limit | Add delays between requests, check rate limit policy |
| 500 | Server Error | Retry later, contact MISO support if persistent |

### Handling 404 Responses

404 responses are treated as "no data available yet" rather than errors. The scraper will return empty data arrays for dates where forecast data hasn't been published.

## Validation

The scraper validates:
- JSON structure integrity
- Required fields presence (node, lmp, mcc, mec, mlc, interval, timeInterval)
- Interval values (1-24 for hourly, 1-288 or HH:MM for 5-minute)
- LMP arithmetic: `LMP = MEC + MCC + MLC` (within 0.01 tolerance)
- Date consistency between candidate and response
- Numeric types for price components

## Differences from Day-Ahead Ex-Ante LMP

| Aspect | Day-Ahead Ex-Ante | Real-Time Ex-Ante |
|--------|-------------------|-------------------|
| **Market** | Day-Ahead | Real-Time |
| **Published** | 2:00 PM EST (day before) | 7:00 AM EST (operating day) |
| **Updates** | Once daily | Continuous (near real-time) |
| **Intervals** | 24 hourly only | 24 hourly OR 288 5-minute |
| **Volume** | ~72K-120K/day | ~72K-400K/day |
| **Forecast Window** | Next day | Same day (imminent) |
| **Use Case** | Planning, hedging | Trading, operations |

## Deduplication Strategy

Uses Redis-based hash registry to avoid re-processing identical data:
- Hash calculated from raw content
- Hash stored with TTL in Redis
- Duplicate content skipped automatically
- Use `--force` flag to override and re-download

## Performance Considerations

### Hourly Resolution
- **API Calls**: ~150-200 pages per day
- **Processing Time**: ~2-3 minutes per day
- **Storage**: ~5-10 MB compressed per day

### 5-Minute Resolution
- **API Calls**: ~500-1000 pages per day
- **Processing Time**: ~10-15 minutes per day
- **Storage**: ~20-40 MB compressed per day

### Recommendations
- Use hourly for most use cases
- Use 5-minute only when required for high-frequency analysis
- Consider parallel processing for large date ranges
- Monitor API rate limits closely with 5-minute resolution

## Troubleshooting

### Redis Connection Errors

```bash
# Check Redis is running
redis-cli ping

# Should return PONG
```

### S3 Upload Failures

```bash
# Verify AWS credentials
aws s3 ls s3://your-bucket/

# Check AWS profile
aws configure list --profile your-profile
```

### API Authentication Failures

```bash
# Test API key manually
curl -H "Ocp-Apim-Subscription-Key: YOUR_KEY" \
  "https://apim.misoenergy.org/pricing/v1/real-time/2025-01-20/lmp-exante?pageNumber=1&timeResolution=hourly"
```

## Related Scrapers

- **Day-Ahead Ex-Ante LMP API**: Forecasted day-ahead prices
- **Day-Ahead Ex-Post LMP**: Actual settled day-ahead prices
- **Real-Time Ex-Post LMP**: Actual settled real-time prices (when available)

## Support

For issues or questions:
1. Check MISO API documentation
2. Verify prerequisites are met
3. Review error logs with `--log-level DEBUG`
4. Contact MISO support for API-specific issues

## Version History

- **1.0.0** (2025-12-05): Initial release
  - Hourly and 5-minute resolution support
  - Full pagination handling
  - Comprehensive validation
  - Redis deduplication
  - S3 storage with date partitioning
