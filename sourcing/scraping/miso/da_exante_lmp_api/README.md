# MISO Day-Ahead Ex-Ante LMP API Scraper

Collects forecasted Day-Ahead Locational Marginal Pricing (LMP) data from MISO's official Pricing API.

## Overview

This scraper retrieves **Ex-Ante (forecasted)** LMP data for the day-ahead energy market across all MISO commercial pricing nodes. Ex-Ante prices represent predicted market prices published before market clearing, used for pre-market trading strategies, generation planning, and risk management.

**Key Distinction**: This is the **official API version** of the Ex-Ante LMP scraper. A separate CSV-based scraper exists for legacy data sources. This API scraper is the preferred method for production use.

### Data Characteristics

- **Source**: MISO Pricing API
- **Endpoint**: `https://apim.misoenergy.org/pricing/v1/day-ahead/{date}/lmp-exante`
- **Data Type**: Forecasted Locational Marginal Prices (Ex-Ante)
- **Publication Time**: 2:00 PM EST the day before the operating day
- **Temporal Resolution**: Hourly (24 intervals per day)
- **Spatial Coverage**: All Commercial Pricing Nodes (~3,000-5,000 nodes)
- **Expected Volume**: ~72,000-120,000 records per day
- **Data Format**: JSON (paginated)

### Business Value

- **Pre-Market Trading**: Develop trading strategies based on forecasted prices
- **Generation Planning**: Optimize generator dispatch and commitment
- **Load Management**: Prepare demand-response strategies
- **Risk Management**: Assess potential market exposures
- **Price Forecasting**: Compare forecasts with actual settled prices (Ex-Post)
- **Scenario Modeling**: Build predictive market scenarios

## Data Schema

### Response Structure

```json
{
  "data": [
    {
      "interval": "1",
      "timeInterval": {
        "resolution": "daily",
        "start": "2023-06-28 00:00:00.000",
        "end": "2023-06-29 00:00:00.000",
        "value": "2023-06-29"
      },
      "node": "ALTW.WELLS1",
      "lmp": 22.1,
      "mcc": 0.03,
      "mec": 21.34,
      "mlc": 0.73
    }
  ],
  "page": {
    "pageNumber": 1,
    "pageSize": 100,
    "totalElements": 72000,
    "totalPages": 720,
    "lastPage": false
  }
}
```

### Field Descriptions

| Field | Type | Description |
|-------|------|-------------|
| `interval` | string | Hour of day (1-24) |
| `timeInterval.value` | string | Operating date (YYYY-MM-DD) |
| `node` | string | Commercial Pricing Node identifier |
| `lmp` | float | Forecasted Locational Marginal Price ($/MWh) |
| `mec` | float | Forecasted Marginal Energy Cost ($/MWh) |
| `mcc` | float | Forecasted Marginal Congestion Cost ($/MWh) |
| `mlc` | float | Forecasted Marginal Loss Cost ($/MWh) |

**LMP Arithmetic**: `LMP = MEC + MCC + MLC`

**Note**: All values are **forecasts** and may differ from final settled prices (Ex-Post).

## Installation

### Prerequisites

- Python 3.8+
- Redis server (for deduplication)
- AWS S3 bucket (for storage)
- MISO API subscription key

### Dependencies

```bash
pip install boto3 click redis requests
```

### Environment Variables

```bash
# Required
export MISO_API_KEY=your_subscription_key_here

# Optional (can be passed as CLI arguments)
export S3_BUCKET=your-bucket-name
export AWS_PROFILE=your-aws-profile
export REDIS_HOST=localhost
export REDIS_PORT=6379
```

## Usage

### Basic Usage

Collect data for a specific date range:

```bash
python scraper_miso_da_exante_lmp_api.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-01 \
    --end-date 2025-01-31
```

### Using Environment Variables

```bash
export MISO_API_KEY=your_key
export S3_BUCKET=your-bucket

python scraper_miso_da_exante_lmp_api.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-31
```

### Single Day Collection

```bash
python scraper_miso_da_exante_lmp_api.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-20 \
    --end-date 2025-01-20
```

### Force Re-download

Re-download data even if it already exists:

```bash
python scraper_miso_da_exante_lmp_api.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-01 \
    --end-date 2025-01-02 \
    --force
```

### Debug Mode

Enable detailed logging:

```bash
python scraper_miso_da_exante_lmp_api.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-20 \
    --end-date 2025-01-20 \
    --log-level DEBUG
```

### Custom Environment

Specify production environment:

```bash
python scraper_miso_da_exante_lmp_api.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --environment prod
```

## CLI Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--api-key` | string | `$MISO_API_KEY` | MISO API subscription key |
| `--start-date` | date | Required | Start date (YYYY-MM-DD) |
| `--end-date` | date | Required | End date (YYYY-MM-DD) |
| `--s3-bucket` | string | `$S3_BUCKET` | S3 bucket for storage |
| `--aws-profile` | string | `$AWS_PROFILE` | AWS profile name |
| `--redis-host` | string | `localhost` | Redis host |
| `--redis-port` | int | `6379` | Redis port |
| `--redis-db` | int | `0` | Redis database number |
| `--environment` | choice | `dev` | Environment (dev/staging/prod) |
| `--force` | flag | False | Force re-download |
| `--skip-hash-check` | flag | False | Skip deduplication |
| `--log-level` | choice | `INFO` | Logging level (DEBUG/INFO/WARNING/ERROR) |

## Output

### S3 Storage

Files are stored in S3 with the following structure:

```
s3://your-bucket/sourcing/miso_da_exante_lmp_api/
    ├── date=2025-01-01/
    │   └── da_exante_lmp_api_20250101.json.gz
    ├── date=2025-01-02/
    │   └── da_exante_lmp_api_20250102.json.gz
    └── date=2025-01-03/
        └── da_exante_lmp_api_20250103.json.gz
```

### File Format

Each file contains:
- **data**: Array of forecast records (all nodes × 24 intervals)
- **total_records**: Total number of records
- **total_pages**: Number of API pages fetched
- **metadata**: Collection metadata

### Data Volume

- **Per Day**: ~72,000-120,000 forecast records
- **File Size**: ~5-15 MB per day (gzip compressed)
- **Collection Time**: 1-3 minutes per day (depending on pagination)

## Testing

### Run All Tests

```bash
pytest tests/ -v
```

### Run Specific Test Suite

```bash
pytest tests/test_scraper_miso_da_exante_lmp_api.py::TestCandidateGeneration -v
```

### Test Coverage

```bash
pytest tests/ --cov=scraper_miso_da_exante_lmp_api --cov-report=html
```

## Architecture

### Collection Framework

This scraper extends the `BaseCollector` framework and implements:

1. **generate_candidates()**: Creates download candidates for each date
2. **collect_content()**: Fetches paginated JSON data from API
3. **validate_content()**: Validates JSON structure and data quality

### Key Features

- **Pagination Handling**: Automatically fetches all pages
- **Hash Deduplication**: Uses Redis to prevent duplicate downloads
- **S3 Storage**: Uploads to S3 with date partitioning
- **Error Handling**: Comprehensive error handling for API failures
- **Data Validation**: Validates LMP arithmetic and data consistency
- **Kafka Notifications**: Publishes collection events to Kafka

### Data Flow

```
1. Generate candidates (one per date)
2. Check Redis hash registry (skip if exists)
3. Fetch data from API (with pagination)
4. Validate JSON structure and data quality
5. Upload to S3 (gzip compressed)
6. Update Redis hash registry
7. Publish Kafka notification
```

## Authentication

### Obtaining API Key

1. Register at MISO API developer portal
2. Subscribe to Pricing API product
3. Retrieve subscription key
4. Store securely (never commit to version control)

### Security Best Practices

- Use environment variables for API keys
- Rotate keys periodically
- Never commit keys to version control
- Use HTTPS only
- Implement key storage best practices

## Rate Limiting

The MISO API may enforce rate limits. Recommended practices:

- Monitor for 429 responses (rate limit exceeded)
- Implement exponential backoff for retries
- Add delays between requests if needed
- Cache responses to minimize requests

## Troubleshooting

### Common Issues

**401 Unauthorized**
- Verify API key is correct
- Check key has proper subscription to Pricing API
- Ensure key is not expired

**404 Not Found**
- Data may not be available for requested date yet
- Check if date is in valid range
- Verify date format is YYYY-MM-DD

**429 Rate Limit**
- Add delays between requests
- Implement exponential backoff
- Contact MISO for rate limit increase

**Timeout Errors**
- Increase timeout value (default: 180 seconds)
- MISO API can be slow with large paginated responses
- Check network connectivity

**Validation Failures**
- Check if API response format has changed
- Verify LMP arithmetic (LMP = MEC + MCC + MLC)
- Review logs for specific validation errors

### Debug Mode

Enable debug logging for detailed diagnostics:

```bash
python scraper_miso_da_exante_lmp_api.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-20 \
    --end-date 2025-01-20 \
    --log-level DEBUG
```

## Comparison: Ex-Ante vs Ex-Post

| Aspect | Ex-Ante (Forecast) | Ex-Post (Settled) |
|--------|-------------------|-------------------|
| **Timing** | Published day before | Published after market clearing |
| **Nature** | Forecasted prices | Final settled prices |
| **Endpoint** | `/lmp-exante` | `/lmp-expost` |
| **Use Case** | Pre-market planning | Financial settlement |
| **Accuracy** | Predictive | Definitive |
| **Scraper** | This scraper | `scraper_miso_da_expost_lmp.py` |

### Comparative Analysis

Compare Ex-Ante forecasts with Ex-Post settled prices to:
- Measure forecast accuracy
- Identify prediction errors
- Build machine learning models
- Understand market dynamics
- Improve forecasting techniques

## Version History

- **v1.0** (2025-12-05): Initial release
  - API-based collection with pagination
  - Full validation and error handling
  - S3 storage with date partitioning
  - Redis deduplication
  - Comprehensive test coverage

## Related Scrapers

- **Ex-Post LMP API**: `scraper_miso_da_expost_lmp.py` - Settled prices
- **Ex-Ante LMP CSV**: `scraper_miso_da_exante_lmp.py` - Legacy CSV version
- **Ex-Ante ASM MCP**: `scraper_miso_da_exante_asm_mcp.py` - Ancillary services prices

## Support

For issues or questions:
1. Check this README
2. Review logs with `--log-level DEBUG`
3. Verify MISO API documentation
4. Contact team for assistance

## License

Internal use only - MISO data subject to their terms of service.
