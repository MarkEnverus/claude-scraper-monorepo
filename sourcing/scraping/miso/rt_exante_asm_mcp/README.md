# MISO Real-Time Ex-Ante ASM MCP Scraper

Collects Real-Time Ex-Ante (forecasted) Ancillary Services Market Clearing Prices from MISO's Pricing API.

## Overview

This scraper retrieves forecasted market clearing prices for various ancillary service products across MISO's reserve zones for the real-time market. Data is published at 7:00 AM EST on the operating day and updated continuously throughout the day.

**API Endpoint**: `https://apim.misoenergy.org/pricing/v1/real-time/{date}/asm-exante`

**Data Characteristics**:
- **Products**: 6 types (Regulation, Spin, Supplemental, STR, Ramp-up, Ramp-down)
- **Zones**: 8 reserve zones (Zone 1-8)
- **Time Resolutions**: Hourly (24 intervals/day) or 5-minute (288 intervals/day)
- **Expected Volume**: ~1,152 records/day (hourly) or ~13,824 records/day (5-minute)
- **Publication**: 7:00 AM EST on operating day
- **Updates**: Continuous updates throughout the day

## Data Products

### Ancillary Service Products

1. **Regulation**: Continuous adjustment of generation to match load
2. **Spin**: Synchronized reserve ready within 10 minutes
3. **Supplemental**: Non-synchronized reserve within 10-30 minutes
4. **STR**: Short-Term Reserve for quick response
5. **Ramp-up**: Reserves for increasing generation
6. **Ramp-down**: Reserves for decreasing generation

### Reserve Zones

- **Zone 1-8**: Geographic and transmission regions within MISO's footprint

## Installation

### Prerequisites

- Python 3.8+
- Redis server (for deduplication)
- AWS credentials (for S3 storage)
- MISO API subscription key

### Environment Variables

```bash
export MISO_API_KEY="your-miso-api-key"
export S3_BUCKET="your-s3-bucket"
export AWS_PROFILE="your-aws-profile"  # Optional
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
```

### Dependencies

Managed via the parent project's dependency system. Key dependencies:
- `requests` - HTTP client
- `boto3` - AWS S3 client
- `redis` - Redis client
- `click` - CLI framework

## Usage

### Basic Usage

Collect data for a date range with hourly resolution:

```bash
python scraper_miso_rt_exante_asm_mcp.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --time-resolution hourly
```

### 5-Minute Resolution

Collect high-frequency data with 5-minute intervals:

```bash
python scraper_miso_rt_exante_asm_mcp.py \
    --start-date 2025-01-20 \
    --end-date 2025-01-20 \
    --time-resolution 5min
```

### Using Environment Variables

```bash
# Set credentials once
export MISO_API_KEY=your_key
export S3_BUCKET=your-bucket

# Run scraper
python scraper_miso_rt_exante_asm_mcp.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-31
```

### Force Re-download

Force re-download of existing data:

```bash
python scraper_miso_rt_exante_asm_mcp.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-02 \
    --force
```

### Debug Mode

Enable detailed logging for troubleshooting:

```bash
python scraper_miso_rt_exante_asm_mcp.py \
    --start-date 2025-01-20 \
    --end-date 2025-01-20 \
    --log-level DEBUG
```

## Command-Line Options

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| `--api-key` | MISO API subscription key | `$MISO_API_KEY` | Yes |
| `--start-date` | Start date (YYYY-MM-DD) | - | Yes |
| `--end-date` | End date (YYYY-MM-DD) | - | Yes |
| `--time-resolution` | Time resolution: `5min` or `hourly` | `hourly` | No |
| `--s3-bucket` | S3 bucket for storage | `$S3_BUCKET` | No |
| `--aws-profile` | AWS profile name | `$AWS_PROFILE` | No |
| `--redis-host` | Redis host | `localhost` | No |
| `--redis-port` | Redis port | `6379` | No |
| `--redis-db` | Redis database number | `0` | No |
| `--environment` | Environment: `dev`, `staging`, `prod` | `dev` | No |
| `--force` | Force re-download | `False` | No |
| `--skip-hash-check` | Skip deduplication | `False` | No |
| `--log-level` | Logging level | `INFO` | No |

## Data Format

### API Response Structure

```json
{
  "data": [
    {
      "interval": "1",
      "timeInterval": {
        "resolution": "hourly",
        "start": "2024-01-01T00:00:00.0000000+00:00",
        "end": "2024-01-01T01:00:00.0000000+00:00",
        "value": "2024-01-01"
      },
      "product": "Regulation",
      "zone": "Zone 1",
      "mcp": 6.48
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

### Stored Data Format

Data is stored in S3 as gzip-compressed JSON files with combined paginated results:

```json
{
  "data": [/* all records */],
  "total_records": 1152,
  "total_pages": 12,
  "time_resolution": "hourly",
  "metadata": {
    "data_type": "rt_exante_asm_mcp",
    "source": "miso",
    "date": "2024-01-01",
    "date_formatted": "20240101",
    "market_type": "real_time_ancillary_services_exante",
    "time_resolution": "hourly",
    "forecast": true
  }
}
```

## S3 Storage Structure

```
s3://your-bucket/sourcing/
└── miso_rt_exante_asm_mcp/
    └── dev/
        └── year=2025/
            └── month=01/
                └── day=20/
                    └── rt_exante_asm_mcp_hourly_20250120.json.gz
```

## Architecture

### Collection Framework

Uses the `BaseCollector` framework with:
- **Candidate Generation**: One candidate per day
- **Pagination**: Automatic handling of paginated responses
- **Deduplication**: Redis hash-based deduplication
- **Validation**: Comprehensive content validation
- **Storage**: S3 with date partitioning

### Key Classes

- `MisoRealTimeExAnteASMMCPCollector`: Main collector class
  - `generate_candidates()`: Creates download candidates
  - `collect_content()`: Fetches data with pagination
  - `validate_content()`: Validates response structure

## Validation

The scraper validates:

1. **JSON Structure**: Proper top-level structure with `data` array
2. **Required Fields**: All required fields present in each record
3. **Time Interval**: Valid interval values based on resolution
4. **Product Types**: Valid ancillary service products
5. **Zone Values**: Valid reserve zones (Zone 1-8)
6. **MCP Values**: Numeric market clearing prices
7. **Date Consistency**: Data matches requested date

## Testing

### Run All Tests

```bash
pytest sourcing/scraping/miso/rt_exante_asm_mcp/tests/ -v
```

### Run Specific Test Class

```bash
pytest sourcing/scraping/miso/rt_exante_asm_mcp/tests/test_scraper_miso_rt_exante_asm_mcp.py::TestValidateContent -v
```

### Test Coverage

```bash
pytest sourcing/scraping/miso/rt_exante_asm_mcp/tests/ --cov=sourcing.scraping.miso.rt_exante_asm_mcp --cov-report=html
```

## Error Handling

The scraper handles:

- **400 Bad Request**: Invalid date format or parameters
- **401 Unauthorized**: Invalid API key
- **404 Not Found**: No data available for date (warning, not error)
- **429 Rate Limit**: Rate limit exceeded
- **Network Errors**: Request timeouts and connection issues
- **JSON Errors**: Invalid JSON responses

## Comparison with Day-Ahead Ex-Ante ASM MCP

| Feature | Real-Time Ex-Ante | Day-Ahead Ex-Ante |
|---------|-------------------|-------------------|
| **Publication** | 7:00 AM EST on operating day | 2:00 PM EST day before |
| **Updates** | Continuous throughout day | Once per day |
| **Time Resolution** | 5-min or hourly | Daily aggregates |
| **Volume** | 1,152-13,824 records/day | ~192 records/day |
| **Endpoint** | `/real-time/{date}/asm-exante` | `/day-ahead/{date}/asm-exante` |
| **Use Case** | Real-time trading, operations | Day-ahead planning |

## Performance Considerations

### Hourly Resolution
- ~1,152 records per day
- Typical collection time: 30-60 seconds per day
- Recommended for: Daily operations, historical analysis

### 5-Minute Resolution
- ~13,824 records per day
- Typical collection time: 2-5 minutes per day
- Recommended for: High-frequency analysis, real-time monitoring

### Rate Limits
- MISO API has rate limits (not publicly specified)
- Scraper includes 180-second timeout for slow responses
- Consider adding delays between requests for large date ranges

## Troubleshooting

### Common Issues

**Issue**: `Unauthorized - invalid API key`
- **Solution**: Verify `MISO_API_KEY` environment variable is set correctly
- Check API key is active in MISO portal

**Issue**: `Failed to connect to Redis`
- **Solution**: Ensure Redis server is running: `redis-cli ping`
- Check `REDIS_HOST` and `REDIS_PORT` settings

**Issue**: `No data available for date`
- **Solution**: Data may not be published yet (available at 7 AM EST)
- Verify date format is YYYY-MM-DD
- Check if date is within available data range

**Issue**: `Rate limit exceeded`
- **Solution**: Add delays between requests
- Reduce date range per invocation
- Contact MISO for rate limit increase

## Development

### Adding New Features

1. Follow the `BaseCollector` framework patterns
2. Update tests for new functionality
3. Validate against MISO API specifications
4. Update documentation

### Code Quality

Run linting and type checking:

```bash
# Type checking
mypy sourcing/scraping/miso/rt_exante_asm_mcp/scraper_miso_rt_exante_asm_mcp.py

# Style checking
ruff check sourcing/scraping/miso/rt_exante_asm_mcp/scraper_miso_rt_exante_asm_mcp.py
```

## References

- **MISO Pricing API**: [MISO Developer Portal](https://apim.misoenergy.org/)
- **API Documentation**: Contact MISO for detailed API specs
- **Ancillary Services**: [MISO Market Operations](https://www.misoenergy.org/markets-and-operations/)

## Version History

- **1.0.0** (2025-12-05): Initial release
  - Real-Time Ex-Ante ASM MCP collection
  - Hourly and 5-minute resolution support
  - Pagination handling
  - Comprehensive validation

## License

See parent project for license information.

## Support

For issues or questions:
1. Check troubleshooting section above
2. Review MISO API documentation
3. Contact data engineering team
