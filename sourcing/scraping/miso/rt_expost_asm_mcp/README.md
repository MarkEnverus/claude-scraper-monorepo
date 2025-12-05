# MISO Real-Time Ex-Post ASM MCP Scraper

Collects Real-Time Ex-Post Ancillary Services Market (ASM) Market Clearing Prices from MISO's Pricing API.

## Overview

This scraper retrieves historical market clearing prices for MISO's Real-Time Ancillary Services Market across multiple reserve zones. The data represents final, settled prices after the market has cleared and is crucial for financial settlement, market analysis, and performance evaluation.

**API Endpoint**: `https://apim.misoenergy.org/pricing/v1/real-time/{date}/asm-expost`

**Data Availability**: 7:00 AM EST on the day after the operating day

**Data Progression**: Prices initially appear as "Preliminary" and finalize within 3-5 days to "Final" state

## Features

- HTTP REST API collection using BaseCollector framework
- Automatic pagination handling for large datasets
- Support for preliminary and final price states
- Support for multiple ancillary service products
- Support for 5-minute and hourly time resolutions
- Support for 8 reserve zones
- API key authentication via Ocp-Apim-Subscription-Key header
- Redis-based hash deduplication
- S3 storage with date partitioning and gzip compression
- Kafka notifications for downstream processing
- Comprehensive error handling and validation

## Data Structure

### Ancillary Service Products

The scraper supports 6 ancillary service products:
- **Regulation**: Frequency regulation service
- **Spin**: Spinning reserve
- **Supplemental**: Supplemental reserve
- **STR**: Short-Term Reserve
- **Ramp-up**: Ramping capability (upward)
- **Ramp-down**: Ramping capability (downward)

### Reserve Zones

Data is available for 8 reserve zones:
- Zone 1 through Zone 8

### Time Resolutions

- **Hourly**: 24 intervals per day (default)
- **5-minute**: 288 intervals per day

### Price States

- **Preliminary**: Initial estimated prices
- **Final**: Finalized settled prices (available 3-5 days after operating day)

## Expected Data Volume

Data volume varies based on filters:

**Hourly Resolution (unfiltered)**:
- 6 products × 8 zones × 24 hours × 2 states = ~2,304 records/day

**5-Minute Resolution (unfiltered)**:
- 6 products × 8 zones × 288 intervals × 2 states = ~27,648 records/day

**Filtered by product and zone**:
- Hourly: 24-48 records/day (preliminary + final)
- 5-minute: 288-576 records/day (preliminary + final)

## Installation

### Prerequisites

1. **Python 3.8+**
2. **Redis** (for deduplication)
3. **AWS credentials** (for S3 storage)
4. **MISO API key** (from MISO's developer portal)

### Dependencies

```bash
pip install boto3 click redis requests
```

## Configuration

### Environment Variables

```bash
# Required
export MISO_API_KEY=your_subscription_key_here

# Optional
export S3_BUCKET=your-s3-bucket
export AWS_PROFILE=your-aws-profile
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB=0
```

### Obtaining MISO API Key

1. Register at MISO's API developer portal
2. Subscribe to the Pricing API product
3. Retrieve your subscription key
4. Store securely (never commit to version control)

## Usage

### Basic Usage

Collect all data for a date range (hourly resolution by default):

```bash
python scraper_miso_rt_expost_asm_mcp.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-01 \
    --end-date 2025-01-31
```

### Using Environment Variables

```bash
export MISO_API_KEY=your_key
export S3_BUCKET=your-bucket

python scraper_miso_rt_expost_asm_mcp.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-31
```

### 5-Minute Resolution Data

Collect data at 5-minute intervals:

```bash
python scraper_miso_rt_expost_asm_mcp.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --time-resolution 5min
```

### Filter by Price State

Collect only final prices:

```bash
python scraper_miso_rt_expost_asm_mcp.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --preliminary-final Final
```

Collect only preliminary prices:

```bash
python scraper_miso_rt_expost_asm_mcp.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --preliminary-final Preliminary
```

### Filter by Product

Collect data for a specific ancillary service product:

```bash
python scraper_miso_rt_expost_asm_mcp.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --product Regulation
```

Available products: `Regulation`, `Spin`, `Supplemental`, `STR`, `Ramp-up`, `Ramp-down`

### Filter by Zone

Collect data for a specific reserve zone:

```bash
python scraper_miso_rt_expost_asm_mcp.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --zone "Zone 1"
```

Available zones: `Zone 1` through `Zone 8`

### Combined Filters

Collect specific product and zone at 5-minute resolution:

```bash
python scraper_miso_rt_expost_asm_mcp.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-20 \
    --end-date 2025-01-20 \
    --product Regulation \
    --zone "Zone 1" \
    --preliminary-final Final \
    --time-resolution 5min
```

### Force Re-download

Force re-download of existing data:

```bash
python scraper_miso_rt_expost_asm_mcp.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-01 \
    --end-date 2025-01-02 \
    --force
```

### Debug Logging

Enable detailed debug logging:

```bash
python scraper_miso_rt_expost_asm_mcp.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-20 \
    --end-date 2025-01-20 \
    --log-level DEBUG
```

## Command-Line Options

```
--api-key TEXT                  MISO API subscription key (or set MISO_API_KEY env var)
--start-date YYYY-MM-DD        Start date for data collection [required]
--end-date YYYY-MM-DD          End date for data collection [required]
--product TEXT                 Filter by product (Regulation, Spin, Supplemental, STR, Ramp-up, Ramp-down)
--zone TEXT                    Filter by zone (Zone 1-8)
--preliminary-final TEXT       Filter by state (Preliminary, Final)
--time-resolution TEXT         Time resolution (5min, hourly) [default: hourly]
--s3-bucket TEXT               S3 bucket for storage (or set S3_BUCKET env var)
--aws-profile TEXT             AWS profile name (or set AWS_PROFILE env var)
--redis-host TEXT              Redis host [default: localhost]
--redis-port INTEGER           Redis port [default: 6379]
--redis-db INTEGER             Redis database number [default: 0]
--environment TEXT             Environment (dev, staging, prod) [default: dev]
--force                        Force re-download of existing files
--skip-hash-check              Skip Redis hash-based deduplication
--log-level TEXT               Logging level (DEBUG, INFO, WARNING, ERROR) [default: INFO]
--help                         Show this message and exit
```

## Data Format

### API Response Structure

```json
{
  "data": [
    {
      "preliminaryFinal": "Preliminary",
      "product": "Regulation",
      "zone": "Zone 1",
      "mcp": 6.48,
      "timeInterval": {
        "resolution": "5min",
        "start": "2023-06-28 00:00:00.000",
        "end": "2023-06-29 00:00:00.000",
        "value": "2023-06-29"
      }
    }
  ],
  "page": {
    "pageNumber": 1,
    "pageSize": 100,
    "totalElements": 2304,
    "totalPages": 24,
    "lastPage": false
  }
}
```

### Field Descriptions

- **preliminaryFinal**: Price state (`Preliminary` or `Final`)
- **product**: Ancillary service product type
- **zone**: MISO reserve zone identifier
- **mcp**: Market Clearing Price ($/MWh)
- **timeInterval**: Temporal context
  - **resolution**: Time granularity (`5min` or `hourly`)
  - **start**: Start of time interval (EST)
  - **end**: End of time interval (EST)
  - **value**: Operating day date (YYYY-MM-DD)

## S3 Storage Structure

Files are stored in S3 with date partitioning:

```
s3://{bucket}/sourcing/miso_rt_expost_asm_mcp/
  dev/
    file_date=2025-01-20/
      rt_expost_asm_mcp_20250120_hourly.json.gz
      rt_expost_asm_mcp_20250120_final_regulation_zone1_5min.json.gz
  staging/
    ...
  prod/
    ...
```

Files are gzip-compressed JSON with metadata included.

## Redis Deduplication

The scraper uses Redis to track content hashes and avoid re-uploading duplicate data:

- Hash key format: `miso_rt_expost_asm_mcp:{identifier}`
- Content is hashed using SHA-256
- Skip re-upload if hash matches existing data
- Use `--force` to bypass hash check

## Error Handling

### HTTP Status Codes

- **400 Bad Request**: Invalid date format or parameters
- **401 Unauthorized**: Invalid API key
- **404 Not Found**: No data available for requested date (not an error)
- **429 Too Many Requests**: Rate limit exceeded (implement delays)

### Common Issues

**"Unauthorized - invalid API key"**
- Verify your MISO_API_KEY environment variable
- Check key hasn't expired
- Ensure key is subscribed to Pricing API product

**"Failed to connect to Redis"**
- Verify Redis is running: `redis-cli ping`
- Check REDIS_HOST and REDIS_PORT settings
- Ensure Redis is accessible from your network

**"No data available for date"**
- Data is published at 7am EST day after operating day
- Check if date is too far in the past (retention policy)
- Verify date is not in the future

**"Rate limit exceeded"**
- Add delays between requests
- Reduce concurrent execution
- Contact MISO for rate limit increase

## Data Collection Strategy

### Preliminary vs Final Data

For production use, recommended strategy:

1. **Initial Collection**: Run scraper daily at 8am EST for previous day
   - Collects preliminary prices
   - Use `--preliminary-final Preliminary`

2. **Final Collection**: Re-run scraper 5 days later
   - Collects finalized prices
   - Use `--preliminary-final Final`
   - Use `--force` to re-download and replace preliminary data

3. **Comparison**: Track price changes between preliminary and final states
   - Significant changes may indicate settlement adjustments
   - Useful for market analysis

### Scheduling Examples

**Cron job for daily preliminary collection**:
```bash
0 8 * * * /path/to/venv/bin/python /path/to/scraper_miso_rt_expost_asm_mcp.py \
    --start-date $(date -d "yesterday" +\%Y-\%m-\%d) \
    --end-date $(date -d "yesterday" +\%Y-\%m-\%d) \
    --preliminary-final Preliminary
```

**Cron job for final collection (5 days later)**:
```bash
0 9 * * * /path/to/venv/bin/python /path/to/scraper_miso_rt_expost_asm_mcp.py \
    --start-date $(date -d "5 days ago" +\%Y-\%m-\%d) \
    --end-date $(date -d "5 days ago" +\%Y-\%m-\%d) \
    --preliminary-final Final \
    --force
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest sourcing/scraping/miso/rt_expost_asm_mcp/tests/ -v

# Run specific test class
pytest sourcing/scraping/miso/rt_expost_asm_mcp/tests/test_scraper_miso_rt_expost_asm_mcp.py::TestValidateContent -v

# Run with coverage
pytest sourcing/scraping/miso/rt_expost_asm_mcp/tests/ --cov=sourcing.scraping.miso.rt_expost_asm_mcp --cov-report=html
```

## Version Information

- **Infrastructure Version**: 1.3.0
- **Last Updated**: 2025-12-05
- **API Version**: v1

## Related Scrapers

- **Day-Ahead Ex-Ante ASM MCP**: Forward-looking ancillary services prices
- **Day-Ahead Ex-Post LMP**: Settled day-ahead energy prices
- **Real-Time Ex-Ante LMP**: Forward-looking real-time energy prices

## Support

For issues with:
- **API access**: Contact MISO's API support team
- **Scraper bugs**: File issue in repository
- **Data questions**: Consult MISO's data documentation

## License

Internal use only - proprietary data collection infrastructure.

## Change Log

### Version 1.0.0 (2025-12-05)
- Initial release
- Support for preliminary and final price states
- Support for 6 ancillary service products
- Support for 8 reserve zones
- Support for 5-minute and hourly resolutions
- Comprehensive filtering options
- Full pagination support
- Redis deduplication
- S3 storage integration
