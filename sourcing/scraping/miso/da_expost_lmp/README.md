# MISO Day-Ahead Ex-Post LMP Scraper

## Overview

This scraper collects Day-Ahead Ex-Post Locational Marginal Price (LMP) data from MISO's Pricing API. Ex-Post LMP represents the final settled prices for the day-ahead energy market after the market has cleared.

## Data Source

- **API Endpoint**: `https://apim.misoenergy.org/pricing/v1/day-ahead/{date}/lmp-expost`
- **Method**: GET
- **Authentication**: API Key via `Ocp-Apim-Subscription-Key` header
- **Data Format**: JSON (paginated)
- **Update Frequency**: Daily (available at 2:00 PM EST on the day before the operating day)

## Data Characteristics

### Volume
- **Nodes**: ~3,000-5,000 Commercial Pricing Nodes (CPNodes)
- **Intervals**: 24 hourly intervals per day
- **Records per day**: ~72,000-120,000 records
- **Pagination**: Multiple pages (potentially hundreds)

### Price Components

Each record includes:
- **LMP**: Total Locational Marginal Price ($/MWh)
- **MEC**: Marginal Energy Cost (generation cost)
- **MCC**: Marginal Congestion Cost (transmission constraint cost)
- **MLC**: Marginal Loss Cost (electrical loss cost)

**Formula**: `LMP = MEC + MCC + MLC`

### Temporal Information

- **Interval**: Hour-ending interval (1-24)
  - Interval 1 = 00:00-01:00 EST
  - Interval 13 = 12:00-13:00 EST
  - Interval 24 = 23:00-24:00 EST
- **Timezone**: Eastern Standard Time (EST, UTC-05:00) - fixed year-round

## Installation

### Prerequisites

1. **MISO API Key**: Register at MISO's developer portal and obtain an API subscription key
2. **Redis**: Running Redis instance for deduplication
3. **AWS Credentials**: Configured for S3 storage
4. **Python Dependencies**: Install from project requirements

### Environment Variables

```bash
# Required
export MISO_API_KEY=your_subscription_key_here
export S3_BUCKET=your-bucket-name

# Optional
export AWS_PROFILE=your-aws-profile
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB=0
```

## Usage

### Basic Usage

```bash
python scraper_miso_da_expost_lmp.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-31
```

### Advanced Usage

```bash
# Collect single day with debug logging
python scraper_miso_da_expost_lmp.py \
    --api-key YOUR_KEY \
    --start-date 2025-01-20 \
    --end-date 2025-01-20 \
    --s3-bucket your-bucket \
    --log-level DEBUG

# Force re-download existing data
python scraper_miso_da_expost_lmp.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-02 \
    --force

# Use LocalStack for development
python scraper_miso_da_expost_lmp.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-02 \
    --aws-profile localstack \
    --environment dev
```

### Command-Line Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `--api-key` | string | Yes* | - | MISO API subscription key (or `MISO_API_KEY` env var) |
| `--start-date` | date | Yes | - | Start date for collection (YYYY-MM-DD) |
| `--end-date` | date | Yes | - | End date for collection (YYYY-MM-DD) |
| `--s3-bucket` | string | Yes* | - | S3 bucket for data storage (or `S3_BUCKET` env var) |
| `--aws-profile` | string | No | - | AWS profile name (or `AWS_PROFILE` env var) |
| `--redis-host` | string | No | localhost | Redis host for deduplication |
| `--redis-port` | int | No | 6379 | Redis port |
| `--redis-db` | int | No | 0 | Redis database number |
| `--environment` | choice | No | dev | Environment (dev/staging/prod) |
| `--force` | flag | No | False | Force re-download of existing files |
| `--skip-hash-check` | flag | No | False | Skip Redis hash-based deduplication |
| `--log-level` | choice | No | INFO | Logging level (DEBUG/INFO/WARNING/ERROR) |

\* Can be provided via environment variable

## Output Format

### S3 Storage Structure

```
s3://bucket-name/sourcing/
    year=2025/
        month=01/
            day=20/
                da_expost_lmp_20250120.json.gz
```

### JSON Structure

```json
{
  "data": [
    {
      "interval": "1",
      "timeInterval": {
        "resolution": "daily",
        "start": "2025-01-19 00:00:00.000",
        "end": "2025-01-20 00:00:00.000",
        "value": "2025-01-20"
      },
      "node": "ALTW.WELLS1",
      "lmp": 21.60,
      "mcc": 0.02,
      "mec": 20.94,
      "mlc": 0.64
    }
  ],
  "total_records": 72000,
  "total_pages": 720,
  "metadata": {
    "data_type": "da_expost_lmp",
    "source": "miso",
    "date": "2025-01-20",
    "date_formatted": "20250120",
    "market_type": "day_ahead_energy_expost"
  }
}
```

## Data Quality Checks

The scraper performs comprehensive validation:

1. **Required Fields**: Verifies all required fields are present
2. **LMP Arithmetic**: Validates `LMP = MEC + MCC + MLC` (within ±0.01 tolerance)
3. **Interval Range**: Ensures intervals are 1-24
4. **Date Consistency**: Verifies all records match the requested date
5. **Numeric Values**: Confirms LMP components are numeric
6. **Data Volume**: Warns if record count is unexpectedly low

## Performance Considerations

### Collection Time
- **Complete daily dataset**: 10-30 minutes
- **Single page**: 2-5 seconds (MISO API can be slow)
- **Pagination**: Hundreds of sequential requests may be needed

### Rate Limiting
- The scraper respects MISO's API rate limits
- Implements exponential backoff for 429 responses
- Consider adding delays between pages if needed

### Data Volume
- **Raw JSON**: ~50-100 MB per day
- **Gzip compressed**: ~5-10 MB per day
- **Storage**: ~3.5 GB/year (compressed)

## Testing

Run the test suite:

```bash
# Run all tests
pytest sourcing/scraping/miso/da_expost_lmp/tests/ -v

# Run with coverage
pytest sourcing/scraping/miso/da_expost_lmp/tests/ --cov=sourcing.scraping.miso.da_expost_lmp --cov-report=html

# Run specific test
pytest sourcing/scraping/miso/da_expost_lmp/tests/test_scraper_miso_da_expost_lmp.py::TestMisoDayAheadExPostLMPCollector::test_collect_content_pagination -v
```

## Troubleshooting

### Common Issues

#### 401 Unauthorized
- **Cause**: Invalid or expired API key
- **Solution**: Verify `MISO_API_KEY` is correct and active

#### 404 Not Found
- **Cause**: Data not yet available for the requested date
- **Solution**: Data is available at 2:00 PM EST the day before. Try earlier dates.

#### 429 Rate Limit Exceeded
- **Cause**: Too many requests to MISO API
- **Solution**: The scraper implements exponential backoff. Consider adding delays or reducing concurrency.

#### Slow Performance
- **Cause**: MISO API can be slow, especially with large paginated responses
- **Solution**: This is expected. Use `--log-level DEBUG` to monitor progress.

#### Redis Connection Error
- **Cause**: Redis not running or incorrect connection details
- **Solution**: Start Redis (`redis-server`) and verify connection settings

## Business Value

This data enables:

- **Financial Settlement**: Accurate financial reconciliation for day-ahead transactions
- **Market Analysis**: Price analysis, forecasting, and trend identification
- **Regulatory Compliance**: Auditable pricing data for reporting
- **Performance Benchmarking**: Compare ex-ante (forecast) vs ex-post (actual) prices
- **Risk Management**: Historical price patterns for risk modeling

## Related Scrapers

- **Day-Ahead Ex-Ante LMP**: Forecast prices before market clears
- **Day-Ahead Ex-Ante ASM MCP**: Ancillary services market clearing prices
- **Real-Time Ex-Post LMP**: Real-time settled prices

## Support

For issues or questions:
1. Check the specification document: `/tmp/miso-pricing-specs/spec-day-ahead-expost-lmp.md`
2. Review MISO API documentation: https://docs.misoenergy.org
3. Contact the data engineering team

## Version History

- **1.0.0** (2025-12-05): Initial release
  - Full pagination support
  - LMP arithmetic validation
  - Comprehensive error handling
  - Redis deduplication
  - S3 storage with gzip compression
