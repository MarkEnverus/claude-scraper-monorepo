# MISO LMP Consolidated Table Scraper

Collects consolidated Locational Marginal Pricing (LMP) data from MISO's Public API.

## Overview

This scraper retrieves real-time consolidated LMP data across three timeframes in a single API call:
- **FiveMinLMP**: Real-time 5-minute locational marginal pricing
- **HourlyIntegratedLMP**: Hourly integrated pricing data
- **DayAheadExAnteLMP**: Day-ahead ex-ante pricing forecasts

Each timeframe section contains pricing nodes with detailed LMP component breakdowns.

## API Endpoint

- **URL**: `https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable`
- **Method**: GET
- **Authentication**: None (public API)
- **Rate Limits**: None specified (recommended: implement exponential backoff)
- **Update Frequency**: Real-time (every 5 minutes)

## Data Structure

### Response Format

```json
{
  "FiveMinLMP": [
    {
      "Timestamp": "HE 9 INT 15",
      "Region": "South",
      "PricingNodes": [
        {
          "Name": "EES.EXXOBMT",
          "LMP": 25.50,
          "MEC": 17.40,
          "MLC": 2.35,
          "MCC": 5.75
        }
      ]
    }
  ],
  "HourlyIntegratedLMP": [...],
  "DayAheadExAnteLMP": [...]
}
```

### LMP Components

- **LMP** (Locational Marginal Price): Total price in USD/MWh
- **MEC** (Marginal Energy Component): Base energy cost
- **MLC** (Marginal Loss Component): Transmission loss cost
- **MCC** (Marginal Congestion Component): Congestion cost

**Formula**: `LMP = MEC + MLC + MCC`

### Timestamp Formats

- **FiveMinLMP**: `"HE X INT Y"` (e.g., "HE 9 INT 15")
  - HE = Hour Ending (1-24)
  - INT = 5-minute interval (5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60)

- **HourlyIntegratedLMP**: `"HE X"` (e.g., "HE 9")
  - Hourly aggregated values

- **DayAheadExAnteLMP**: `"YYYY-MM-DD HE X"` (e.g., "2025-12-05 HE 1")
  - Forecasted prices for next trading day

## Features

- HTTP REST API collection using BaseCollector framework
- No authentication required (public API)
- Real-time consolidated data across 3 timeframes
- Redis-based hash deduplication
- S3 storage with timestamp partitioning and gzip compression
- Kafka notifications for downstream processing
- Comprehensive error handling and validation
- LMP arithmetic validation (LMP = MEC + MCC + MLC)

## Installation

Ensure infrastructure dependencies are installed:

```bash
# Install required packages
pip install boto3 click redis requests

# Ensure infrastructure files exist
# sourcing/infrastructure/collection_framework.py
# sourcing/infrastructure/hash_registry.py
```

## Usage

### Basic Usage

```bash
# Collect current consolidated LMP data
python scraper_miso_lmp_consolidated.py
```

### With Environment Variables

```bash
# Set environment variables
export S3_BUCKET=your-bucket-name
export REDIS_HOST=localhost
export REDIS_PORT=6379

# Run scraper
python scraper_miso_lmp_consolidated.py
```

### With AWS Profile

```bash
python scraper_miso_lmp_consolidated.py \
    --aws-profile production \
    --environment prod
```

### Force Re-download

```bash
# Force re-download even if content hash exists
python scraper_miso_lmp_consolidated.py --force
```

### Debug Mode

```bash
python scraper_miso_lmp_consolidated.py --log-level DEBUG
```

## Command-Line Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `--s3-bucket` | `S3_BUCKET` | None | S3 bucket for data storage |
| `--aws-profile` | `AWS_PROFILE` | None | AWS profile name |
| `--redis-host` | `REDIS_HOST` | localhost | Redis host for deduplication |
| `--redis-port` | `REDIS_PORT` | 6379 | Redis port |
| `--redis-db` | `REDIS_DB` | 0 | Redis database number |
| `--environment` | - | dev | Environment (dev/staging/prod) |
| `--force` | - | False | Force re-download existing files |
| `--skip-hash-check` | - | False | Skip Redis hash deduplication |
| `--log-level` | - | INFO | Logging level (DEBUG/INFO/WARNING/ERROR) |

## Testing

Run the test suite:

```bash
# Run all tests
pytest sourcing/scraping/miso/lmp_consolidated/tests/ -v

# Run with coverage
pytest sourcing/scraping/miso/lmp_consolidated/tests/ --cov=sourcing.scraping.miso.lmp_consolidated --cov-report=html

# Run specific test class
pytest sourcing/scraping/miso/lmp_consolidated/tests/test_scraper_miso_lmp_consolidated.py::TestCandidateGeneration -v
```

## Data Storage

### S3 Structure

```
s3://bucket-name/sourcing/
  └── miso_lmp_consolidated/
      ├── year=2025/
      │   ├── month=12/
      │   │   ├── day=05/
      │   │   │   ├── lmp_consolidated_20251205_143000.json.gz
      │   │   │   ├── lmp_consolidated_20251205_143500.json.gz
      │   │   │   └── lmp_consolidated_20251205_144000.json.gz
```

### Redis Deduplication

The scraper uses Redis for content-based hash deduplication:

- **Key Pattern**: `hash:{dgroup}:{content_hash}`
- **Purpose**: Prevent duplicate uploads of identical content
- **TTL**: Configurable (default: 7 days)

Content hash is calculated from the JSON response body before upload.

## Error Handling

### HTTP 404 - No Data Available

When the API returns 404 (no data currently available), the scraper creates an empty response structure:

```json
{
  "FiveMinLMP": [],
  "HourlyIntegratedLMP": [],
  "DayAheadExAnteLMP": [],
  "note": "No data available"
}
```

### HTTP 503 - Service Unavailable

API temporarily unavailable - scraper raises `ScrapingError` and retries can be handled at orchestration level.

### JSON Decode Error

Invalid JSON response raises `ScrapingError` with details.

## Validation

The scraper validates:

1. **Structure**: All three required sections present (FiveMinLMP, HourlyIntegratedLMP, DayAheadExAnteLMP)
2. **Timestamp**: Valid timestamp format for each section
3. **Region**: Region field present in each time interval group
4. **Pricing Nodes**: PricingNodes array present and valid
5. **Node Fields**: Required fields (Name, LMP, MLC, MCC) present and numeric
6. **LMP Arithmetic**: If MEC present, validates `LMP = MEC + MLC + MCC` (within 0.01 tolerance)

## Performance

- **Response Time**: Typically < 500ms
- **Response Size**: Approximately 50-200 KB
- **Recommended Timeout**: 30 seconds (default)
- **Collection Frequency**: Every 5 minutes (matches API update frequency)

## Scheduling

### Cron Example (Every 5 Minutes)

```bash
*/5 * * * * cd /path/to/scraper && python scraper_miso_lmp_consolidated.py >> /var/log/miso_lmp_consolidated.log 2>&1
```

### Airflow DAG Example

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'miso_lmp_consolidated',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
)

collect_task = BashOperator(
    task_id='collect_lmp_consolidated',
    bash_command='python /path/to/scraper_miso_lmp_consolidated.py',
    dag=dag,
)
```

## Architecture

### Class: `MisoLmpConsolidatedCollector`

Extends `BaseCollector` from the collection framework.

#### Methods

- `generate_candidates(**kwargs) -> List[DownloadCandidate]`
  - Generates a single candidate for current timestamp
  - No date range support (real-time endpoint)

- `collect_content(candidate: DownloadCandidate) -> bytes`
  - Fetches JSON data from MISO Public API
  - Returns consolidated response with all three sections

- `validate_content(content: bytes, candidate: DownloadCandidate) -> bool`
  - Validates JSON structure and data integrity
  - Checks LMP arithmetic if MEC component present

## Troubleshooting

### Redis Connection Error

```
Failed to connect to Redis: [Errno 61] Connection refused
```

**Solution**: Ensure Redis is running:
```bash
redis-cli ping  # Should return PONG
```

### AWS Credentials Error

```
NoCredentialsError: Unable to locate credentials
```

**Solution**: Configure AWS credentials:
```bash
aws configure --profile your-profile
# OR
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
```

### Empty Sections Warning

```
All sections are empty - no data available
```

**Note**: This is normal - the API occasionally returns empty data during transitions or maintenance. The scraper creates a valid empty structure.

## Related Scrapers

- **da_exante_lmp**: Day-Ahead Ex-Ante LMP (CSV version)
- **da_exante_lmp_api**: Day-Ahead Ex-Ante LMP (API version with pagination)
- **rt_exante_lmp**: Real-Time Ex-Ante LMP
- **nsi_five_minute**: 5-Minute NSI data

## References

- [MISO Public API Documentation](https://www.misoenergy.org/markets-and-operations/real-time--market-data/)
- [LMP Pricing Specification](/tmp/miso-pricing-specs/spec-public-lmpconsolidated.md)
- [BaseCollector Framework](../../infrastructure/collection_framework.py)

## Version Information

- **Infrastructure Version**: 1.3.0
- **Last Updated**: 2025-12-05
- **Python Version**: 3.8+

## License

Internal use only - proprietary data collection pipeline.
