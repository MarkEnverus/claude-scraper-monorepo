# MISO Day-Ahead Ex-Ante Ancillary Services Market Clearing Prices Scraper

Production-ready HTTP/REST API scraper for collecting MISO Day-Ahead Ex-Ante Ancillary Services Market Clearing Prices (ASM MCP) data.

**Status:** ✅ Production Ready
**Version:** 1.3.0
**Infrastructure Version:** 1.3.0
**Created:** 2025-12-03

---

## Quick Start

```bash
# Set environment variables
export MISO_API_KEY="your_api_key"
export S3_BUCKET="your-bucket"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"

# Run scraper
python scraper_miso_da_exante_asm_mcp.py \
  --start-date 2024-12-01 \
  --end-date 2024-12-01
```

---

## Overview

This scraper collects ancillary services market clearing prices from MISO's Day-Ahead Ex-Ante market. The data includes Market Clearing Prices (MCP) for six ancillary service products across eight reserve zones.

### Data Collected

**Products** (6 types):
- Regulation - Regulation reserve service
- Spin - Spinning reserve service
- Supplemental - Supplemental reserve service
- STR - Short-term reserve service
- Ramp-up - Ramp-up capability
- Ramp-down - Ramp-down capability

**Zones** (8 regions):
- Zone 1 through Zone 8

**Frequency:** Daily, with 24 hourly intervals
**Typical Records:** 1,152 records per day (6 products × 8 zones × 24 hours)

---

## API Details

### Endpoint

```
https://apim.misoenergy.org/pricing/v1/day-ahead/{YYYY-MM-DD}/asm-exante
```

### Authentication

```bash
Header: Ocp-Apim-Subscription-Key: {your-api-key}
```

### API Key

Contact MISO at https://api.misoenergy.org/ to obtain an API key.

### Response Format

```json
{
  "data": [
    {
      "interval": "1",
      "timeInterval": {
        "resolution": "hourly",
        "start": "2024-12-01T00:00:00",
        "end": "2024-12-01T01:00:00",
        "value": "1"
      },
      "product": "Regulation",
      "zone": "Zone 1",
      "mcp": 15.0
    }
  ],
  "page": {
    "pageNumber": 1,
    "pageSize": 100,
    "totalElements": 1152,
    "totalPages": 12,
    "lastPage": false
  }
}
```

### Pagination

The scraper automatically handles pagination:
- Starts with `pageNumber=1`
- Fetches subsequent pages until `page.lastPage=true`
- Combines all data into single JSON file

---

## Installation

### Prerequisites

- Python 3.9+
- Redis server running
- AWS credentials configured (for S3)
- MISO API subscription key

### Dependencies

```bash
pip install -r requirements.txt
```

Core dependencies:
- requests
- click
- redis
- boto3

---

## Configuration

### Environment Variables

```bash
# Required
export MISO_API_KEY="your_api_key_here"

# S3 Storage
export S3_BUCKET="your-bucket-name"
export AWS_PROFILE="your-aws-profile"  # Optional

# Redis Deduplication
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
export REDIS_DB="0"
```

---

## Usage

### Basic Usage

```bash
python scraper_miso_da_exante_asm_mcp.py \
  --api-key YOUR_KEY \
  --start-date 2024-12-01 \
  --end-date 2024-12-01
```

### Using Environment Variables

```bash
export MISO_API_KEY="your_key"
python scraper_miso_da_exante_asm_mcp.py \
  --start-date 2024-12-01 \
  --end-date 2024-12-31
```

### Date Range Collection

```bash
python scraper_miso_da_exante_asm_mcp.py \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

### Force Re-download

```bash
python scraper_miso_da_exante_asm_mcp.py \
  --start-date 2024-12-01 \
  --end-date 2024-12-01 \
  --force
```

### Debug Logging

```bash
python scraper_miso_da_exante_asm_mcp.py \
  --start-date 2024-12-01 \
  --end-date 2024-12-01 \
  --log-level DEBUG
```

### CLI Options

```
--api-key TEXT              MISO API subscription key [required]
--start-date YYYY-MM-DD     Start date for collection [required]
--end-date YYYY-MM-DD       End date for collection [required]
--s3-bucket TEXT            S3 bucket name
--aws-profile TEXT          AWS profile name
--redis-host TEXT           Redis host [default: localhost]
--redis-port INT            Redis port [default: 6379]
--redis-db INT              Redis database [default: 0]
--environment TEXT          Environment [dev|staging|prod] [default: dev]
--force                     Force re-download existing files
--skip-hash-check           Skip Redis hash deduplication
--log-level TEXT            Logging level [default: INFO]
--help                      Show help message
```

---

## Storage

### S3 Path Structure

```
s3://{bucket}/sourcing/miso_da_exante_asm_mcp/
  year=2024/
    month=12/
      day=01/
        da_exante_asm_mcp_20241201.json.gz
```

### File Format

- **Format:** JSON (gzip compressed)
- **Size:** ~5 KB per day (compressed)
- **Naming:** `da_exante_asm_mcp_YYYYMMDD.json.gz`

### File Contents

```json
{
  "data": [ ... 1152 records ... ],
  "total_records": 1152,
  "metadata": {
    "data_type": "da_exante_asm_mcp",
    "source": "miso",
    "date": "2024-12-01",
    "date_formatted": "20241201",
    "market_type": "day_ahead_ancillary_services"
  }
}
```

---

## Features

### Automatic Pagination

✅ Automatically fetches all pages
✅ Combines data into single file
✅ No manual pagination required

### Data Validation

✅ Validates all required fields
✅ Checks product types (6 valid values)
✅ Checks zone format (Zone 1-8)
✅ Validates numeric MCP values
✅ Validates time interval structure

### Deduplication

✅ Redis hash-based deduplication
✅ SHA-256 hash of content
✅ 365-day TTL
✅ Prevents duplicate S3 uploads

### Error Handling

✅ HTTP error handling (400, 401, 404, 500)
✅ Network timeout handling (180s timeout)
✅ JSON parsing errors
✅ Validation errors
✅ S3/Redis connection errors

### Logging

✅ Structured logging
✅ Configurable log levels
✅ Progress tracking
✅ Error details with stack traces

---

## Testing

### Run Unit Tests

```bash
pytest tests/ -v
```

### Run with Coverage

```bash
pytest tests/ -v \
  --cov=sourcing.scraping.miso.da_exante_asm_mcp \
  --cov-report=html
```

### Test Results

- **Unit Tests:** 12/12 passed ✅
- **Coverage:** 81% ✅
- **Integration Tests:** All passed ✅

See [INTEGRATION_TEST.md](INTEGRATION_TEST.md) for detailed test results.

---

## Known Issues

### Slow API Response Time

⚠️ **Issue:** MISO API takes ~2 minutes to respond

**Impact:** Collection takes longer than typical APIs

**Resolution:** Timeout increased to 180 seconds

**Workaround:** None needed - scraper handles automatically

---

## Troubleshooting

### Issue: Timeout Error

**Symptom:**
```
requests.exceptions.Timeout: Request timed out
```

**Solution:** API is slow. Timeout is set to 180s. If still timing out, check network connection or increase timeout further.

### Issue: 401 Unauthorized

**Symptom:**
```
HTTP 401 Unauthorized - invalid API key
```

**Solution:**
1. Verify API key is correct
2. Check environment variable: `echo $MISO_API_KEY`
3. Obtain new key from https://api.misoenergy.org/

### Issue: 404 Not Found

**Symptom:**
```
HTTP 404 Not Found - no data available
```

**Solution:** Data may not be published yet. MISO publishes at 2pm EST day before market date.

### Issue: Redis Connection Failed

**Symptom:**
```
Failed to connect to Redis
```

**Solution:**
```bash
# Check Redis is running
redis-cli ping

# Start Redis if needed
redis-server
```

### Issue: S3 Upload Failed

**Symptom:**
```
Failed to upload to S3: NoSuchBucket
```

**Solution:**
```bash
# Create bucket
aws s3 mb s3://your-bucket-name

# Verify credentials
aws sts get-caller-identity
```

---

## Performance

### Collection Metrics

| Metric | Value |
|--------|-------|
| API Response Time | ~114 seconds |
| Records per Day | 1,152 |
| File Size (compressed) | ~5 KB |
| Memory Usage | <50 MB |
| CPU Usage | <5% |

### Estimated Times

| Operation | Time |
|-----------|------|
| Single day | ~2 minutes |
| One month | ~1 hour |
| One year | ~12 hours |

---

## Production Deployment

### Pre-Deployment Checklist

- [ ] API key obtained and tested
- [ ] S3 bucket created and accessible
- [ ] Redis server running and accessible
- [ ] AWS credentials configured
- [ ] Unit tests passing
- [ ] Integration tests passing
- [ ] Monitoring configured
- [ ] Alerting configured

### Deployment Steps

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   export MISO_API_KEY="your_key"
   export S3_BUCKET="your-bucket"
   export REDIS_HOST="your-redis-host"
   ```

3. **Test collection:**
   ```bash
   python scraper_miso_da_exante_asm_mcp.py \
     --start-date $(date +%Y-%m-%d) \
     --end-date $(date +%Y-%m-%d) \
     --environment prod
   ```

4. **Schedule regular collection:**
   ```bash
   # Example cron (run daily at 3pm EST, after data is published)
   0 15 * * * /path/to/venv/bin/python /path/to/scraper_miso_da_exante_asm_mcp.py \
     --start-date $(date +%Y-%m-%d) \
     --end-date $(date +%Y-%m-%d) \
     --environment prod
   ```

---

## Monitoring

### Metrics to Track

- Collection success rate
- API response times
- Record counts per day (should be ~1,152)
- Validation failure rates
- S3 upload success rate
- Redis deduplication effectiveness

### Alerts

Configure alerts for:
- Collection failures
- API timeouts (>180s)
- Validation failures
- S3 upload failures
- Redis connection failures

---

## Support

### Documentation

- [Integration Test Results](INTEGRATION_TEST.md) - Comprehensive test documentation
- [Business Analyst Ticket](../../../docs/BA_TICKET_ASM_MCP.md) - Original requirements

### Issues

Report issues with:
- Error messages (full stack trace)
- Date attempted
- Log output (with `--log-level DEBUG`)
- Environment details

---

## License

See repository root LICENSE file.

---

## Changelog

### Version 1.3.0 (2025-12-03)

- ✅ Initial scraper implementation
- ✅ Full pagination support
- ✅ Comprehensive data validation
- ✅ Redis hash-based deduplication
- ✅ S3 storage with date partitioning
- ✅ Error handling for all scenarios
- ✅ Unit tests (12 tests, 81% coverage)
- ✅ Integration tests (all passing)
- ✅ Production-ready
- ⚠️ Increased timeout to 180s for slow API

---

**Status:** ✅ Production Ready
**Last Updated:** 2025-12-03
**Maintainer:** Data Engineering Team
