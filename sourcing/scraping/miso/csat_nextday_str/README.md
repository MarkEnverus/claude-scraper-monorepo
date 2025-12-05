# MISO CSAT Next-Day Short-Term Reserve Requirement Scraper

Collects next-day short-term reserve requirement data from MISO's public API.

## Overview

The CSAT (Capacity Short of Adequacy Threshold) Next-Day Short-Term Reserve Requirement scraper collects daily reserve requirement data published by MISO. This data includes total short-term reserve requirements, breakdowns by reserve component type (Regulation and Contingency), subregional requirements, and qualitative assessments with uncertainty factors.

MISO publishes this data daily by 2:00 PM EST for the next operating day, providing critical information for grid operators, market participants, and reliability coordinators.

## Data Source

- **API Endpoint**: `https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement`
- **Method**: GET
- **Authentication**: None (public API)
- **Update Frequency**: Daily by 2:00 PM EST
- **Historical Support**: Yes (date-based queries)

## Data Format

The API returns JSON with the following structure:

```json
{
  "operatingDay": "2025-01-01",
  "publishedTimestamp": "2024-12-31T14:00:00Z",
  "totalShortTermReserveRequirement": 2500.5,
  "reserveComponents": [
    {
      "type": "RegulationReserve",
      "value": 800.0,
      "unit": "MW"
    },
    {
      "type": "ContingencyReserve",
      "value": 1700.5,
      "unit": "MW"
    }
  ],
  "subregions": [
    {
      "name": "North",
      "shortTermReserveRequirement": 1200.0,
      "reserveDeficit": 0.0
    },
    {
      "name": "Central",
      "shortTermReserveRequirement": 900.5,
      "reserveDeficit": 15.0
    },
    {
      "name": "South",
      "shortTermReserveRequirement": 400.0,
      "reserveDeficit": 0.0
    }
  ],
  "qualitativeAssessment": {
    "overallAdequacy": "Adequate",
    "uncertaintyFactor": 0.15,
    "recommendedActions": [
      "Monitor wind forecasts",
      "Review thermal unit availability"
    ]
  }
}
```

## Field Descriptions

### Top-Level Fields

- **operatingDay**: The operating day for which the reserve requirements apply (YYYY-MM-DD format)
- **publishedTimestamp**: ISO 8601 timestamp when the data was published
- **totalShortTermReserveRequirement**: Total short-term reserve requirement in MW

### Reserve Components

Each reserve component includes:
- **type**: Component type (e.g., "RegulationReserve", "ContingencyReserve")
- **value**: Reserve requirement in MW
- **unit**: Always "MW" for megawatts

### Subregions

Each subregion includes:
- **name**: Subregion name (e.g., "North", "Central", "South")
- **shortTermReserveRequirement**: Reserve requirement for this subregion in MW
- **reserveDeficit**: Reserve deficit (shortfall) in MW, if any

### Qualitative Assessment

- **overallAdequacy**: Text assessment of overall reserve adequacy (e.g., "Adequate", "Marginal", "Inadequate")
- **uncertaintyFactor**: Numeric uncertainty factor between 0.0 and 1.0 indicating confidence level
- **recommendedActions**: Array of recommended actions or monitoring activities

## Validation

The scraper performs comprehensive validation including:

1. **Required Field Presence**: Verifies all required fields exist
2. **Date Consistency**: Ensures `operatingDay` matches the requested date
3. **Arithmetic Validation**:
   - `totalShortTermReserveRequirement` = sum of `reserveComponents[].value`
   - `totalShortTermReserveRequirement` = sum of `subregions[].shortTermReserveRequirement`
4. **Range Validation**:
   - `uncertaintyFactor` is between 0.0 and 1.0
   - All MW values are non-negative
5. **Data Type Validation**: Ensures units are "MW" and numeric fields are numbers
6. **Structure Validation**: Confirms arrays and objects have expected structure

## Installation

### Prerequisites

- Python 3.9+
- Redis (for deduplication)
- AWS credentials configured (for S3 storage)

### Dependencies

```bash
pip install boto3 click redis requests
```

Or if using the monorepo's dependency management:

```bash
# Install from project root
uv sync
```

## Usage

### Basic Usage

Collect data for a date range:

```bash
python scraper_miso_csat_nextday_str.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-31
```

### With Environment Variables

Set up your environment:

```bash
export S3_BUCKET=your-bucket-name
export AWS_PROFILE=your-aws-profile
export REDIS_HOST=localhost
export REDIS_PORT=6379
```

Then run:

```bash
python scraper_miso_csat_nextday_str.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-31
```

### Advanced Options

**Force re-download existing data:**

```bash
python scraper_miso_csat_nextday_str.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-02 \
    --force
```

**Debug logging:**

```bash
python scraper_miso_csat_nextday_str.py \
    --start-date 2025-01-20 \
    --end-date 2025-01-20 \
    --log-level DEBUG
```

**Skip Redis deduplication:**

```bash
python scraper_miso_csat_nextday_str.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --skip-hash-check
```

### Command-Line Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `--start-date` | - | Required | Start date (YYYY-MM-DD) |
| `--end-date` | - | Required | End date (YYYY-MM-DD) |
| `--s3-bucket` | `S3_BUCKET` | None | S3 bucket for storage |
| `--aws-profile` | `AWS_PROFILE` | None | AWS profile name |
| `--redis-host` | `REDIS_HOST` | `localhost` | Redis host |
| `--redis-port` | `REDIS_PORT` | `6379` | Redis port |
| `--redis-db` | `REDIS_DB` | `0` | Redis database number |
| `--environment` | - | `dev` | Environment (dev/staging/prod) |
| `--force` | - | `False` | Force re-download |
| `--skip-hash-check` | - | `False` | Skip deduplication |
| `--log-level` | - | `INFO` | Log level (DEBUG/INFO/WARNING/ERROR) |

## Storage

### S3 Path Structure

Data is stored in S3 with date-based partitioning:

```
s3://your-bucket/sourcing/miso/csat_nextday_str/
    year=2025/
        month=01/
            day=01/
                csat_nextday_str_20250101.json.gz
            day=02/
                csat_nextday_str_20250102.json.gz
```

### Data Format in S3

- **Format**: JSON with gzip compression
- **Partitioning**: By date (year/month/day)
- **Naming**: `csat_nextday_str_YYYYMMDD.json.gz`

## Testing

Run the test suite:

```bash
# From the scraper directory
pytest tests/ -v

# Run specific test class
pytest tests/test_scraper_miso_csat_nextday_str.py::TestContentValidation -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html
```

### Test Coverage

The test suite includes:

- **Candidate Generation Tests**: Verify correct candidate generation for single and multiple dates
- **Data Collection Tests**: Test successful collection, error handling (404, network errors)
- **Validation Tests**: Comprehensive validation of all data fields and arithmetic checks
- **Arithmetic Validation Tests**: Specific tests for sum validation and tolerance
- **Integration Tests**: End-to-end workflow testing

## Architecture

### Collection Framework

This scraper extends `BaseCollector` from the collection framework, providing:

- **Candidate Generation**: Creates download candidates for each date in the range
- **Content Collection**: Fetches JSON data from the MISO API with proper error handling
- **Content Validation**: Validates structure and arithmetic consistency
- **Deduplication**: Uses Redis hash-based deduplication to avoid re-processing identical data
- **Storage**: Uploads validated data to S3 with date partitioning and compression
- **Notifications**: Optional Kafka notifications for downstream processing

### Data Flow

1. **Candidate Generation**: Generate candidates for date range
2. **Deduplication Check**: Check Redis for existing hash (optional)
3. **Collection**: Fetch data from MISO API
4. **Validation**: Validate structure and arithmetic
5. **Storage**: Upload to S3 with gzip compression
6. **Registry Update**: Record hash in Redis
7. **Notification**: Send Kafka notification (optional)

## Error Handling

The scraper handles various error conditions:

- **404 Not Found**: Data not yet available for requested date (warning, not fatal)
- **400 Bad Request**: Invalid date format
- **429 Rate Limited**: API rate limit exceeded
- **Network Errors**: Connection failures, timeouts
- **Validation Failures**: Invalid JSON, missing fields, arithmetic mismatches

## Monitoring

Key metrics to monitor:

- **Collection Success Rate**: Percentage of successful collections
- **Validation Failures**: Count of validation errors
- **API Response Time**: Time to fetch data from MISO API
- **Data Completeness**: Presence of data for all expected dates
- **Arithmetic Consistency**: Validation of sum calculations

## What is CSAT?

CSAT (Capacity Short of Adequacy Threshold) represents MISO's assessment of short-term reserve requirements needed to maintain grid reliability. Short-term reserves include:

- **Regulation Reserve**: Fast-responding reserves to maintain frequency (seconds to minutes)
- **Contingency Reserve**: Reserves to cover unexpected generator or transmission outages (minutes)

The next-day forecast helps operators plan for adequate reserve procurement and provides early warning of potential adequacy concerns.

## Related Documentation

- [MISO Markets Overview](https://www.misoenergy.org/markets-and-operations/)
- [MISO Reserve Requirements](https://www.misoenergy.org/markets-and-operations/real-time--market-and-operations/contingency-and-regulating-reserves/)
- [BaseCollector Framework Documentation](../../infrastructure/collection_framework.py)

## Support

For issues or questions:
- Check the test suite for examples: `tests/test_scraper_miso_csat_nextday_str.py`
- Review validation logic in the scraper code
- Check MISO API status: https://www.misoenergy.org/markets-and-operations/real-time--market-and-operations/market-system-status/

## Version History

- **v1.0.0** (2025-12-05): Initial release
  - Date-based historical collection
  - Comprehensive validation with arithmetic checks
  - Redis deduplication
  - S3 storage with date partitioning
  - Full test coverage
