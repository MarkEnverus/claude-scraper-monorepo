# MISO Real-Time RSG Commitments Scraper

Collects real-time Resource-Specific Generation (RSG) commitment data from MISO's public API.

## Overview

This scraper collects real-time data on resource commitments from the MISO market operator. RSG commitments represent resources that are scheduled to run for specific periods, including the commitment reason, economic parameters, and actual output levels.

**API Endpoint:** `https://public-api.misoenergy.org/api/RealTimeRSGCommitments`

**Update Frequency:** Every 5 minutes (real-time snapshots)

**Data Format:** JSON

## Features

- Real-time snapshot-based collection
- Optional filtering by resource type and commitment reason
- Comprehensive data validation (economic constraints, datetime logic, enum values)
- Redis-based hash deduplication
- S3 storage with gzip compression and date partitioning
- Kafka notifications for downstream processing
- Extensive test coverage (100+ test cases)

## Data Structure

### API Response Format

```json
{
  "data": [
    {
      "resourceId": "ALTW.WELLS1",
      "resourceName": "Wells 1",
      "resourceType": "GENERATOR",
      "commitmentStart": "2025-12-05T10:00:00-05:00",
      "commitmentEnd": "2025-12-05T12:00:00-05:00",
      "commitmentReason": "MUST_RUN",
      "economicMaximum": 100.0,
      "economicMinimum": 20.0,
      "actualOutput": 50.0,
      "marginalCost": 25.5,
      "startupCost": 1000.0,
      "minimumRunTime": 60
    }
  ],
  "metadata": {
    "timestamp": "2025-12-05T10:05:00-05:00",
    "totalCommitments": 150,
    "updateInterval": "PT5M"
  }
}
```

### Resource Types

- `GENERATOR` - Traditional generator units
- `COMBINED_CYCLE` - Combined cycle gas turbine units
- `WIND` - Wind generation facilities
- `SOLAR` - Solar generation facilities
- `ENERGY_STORAGE` - Battery storage systems
- `DEMAND_RESPONSE` - Demand response resources

### Commitment Reasons

- `MUST_RUN` - Required to maintain system reliability
- `RELIABILITY_MUST_RUN` - RMR unit commitment
- `OUT_OF_MERIT_ECONOMIC` - Economic dispatch outside merit order
- `OUT_OF_MERIT_RELIABILITY` - Reliability dispatch outside merit order
- `CONGESTION_RELIEF` - Committed for congestion management
- `LOCAL_VOLTAGE_SUPPORT` - Required for voltage support

## Installation

### Prerequisites

- Python 3.9+
- Redis server (for hash deduplication)
- AWS credentials configured (for S3 access)
- Required Python packages (see requirements)

### Required Packages

```bash
pip install boto3 click redis requests
```

## Usage

### Basic Usage

Collect current RSG commitments snapshot:

```bash
python scraper_miso_rsg_commitments.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack
```

### With Filters

Filter by resource type:

```bash
python scraper_miso_rsg_commitments.py \
  --s3-bucket scraper-testing \
  --resource-type WIND
```

Filter by commitment reason:

```bash
python scraper_miso_rsg_commitments.py \
  --s3-bucket scraper-testing \
  --commitment-reason MUST_RUN
```

Combine multiple filters:

```bash
python scraper_miso_rsg_commitments.py \
  --s3-bucket scraper-testing \
  --resource-type GENERATOR \
  --commitment-reason RELIABILITY_MUST_RUN
```

### Configuration Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `--s3-bucket` | `S3_BUCKET` | (required) | S3 bucket for storing data |
| `--aws-profile` | `AWS_PROFILE` | None | AWS profile to use |
| `--environment` | - | `dev` | Environment (dev/staging/prod) |
| `--redis-host` | `REDIS_HOST` | `localhost` | Redis server host |
| `--redis-port` | `REDIS_PORT` | `6379` | Redis server port |
| `--redis-db` | `REDIS_DB` | `0` | Redis database number |
| `--resource-type` | - | None | Filter by resource type |
| `--commitment-reason` | - | None | Filter by commitment reason |
| `--log-level` | - | `INFO` | Logging level |
| `--kafka-connection-string` | `KAFKA_CONNECTION_STRING` | None | Kafka connection for notifications |

## S3 Storage Structure

Data is stored with date partitioning:

```
s3://{bucket}/sourcing/miso_rsg_commitments/
  year=2025/
    month=12/
      day=05/
        rsg_commitments_20251205_1005.json.gz
        rsg_commitments_20251205_1010.json.gz
        rsg_commitments_type_wind_20251205_1015.json.gz
```

Files are automatically compressed with gzip before upload.

## Data Validation

The scraper performs comprehensive validation on collected data:

### Structure Validation
- Response must be a dict with 'data' and 'metadata' keys
- 'data' must be a list of commitment records
- 'metadata' must contain required fields (timestamp, totalCommitments, updateInterval)
- totalCommitments must match actual data array length

### Record Validation
- All required fields must be present
- resourceType must be one of valid values
- commitmentReason must be one of valid values
- Numeric fields must be valid numbers
- minimumRunTime must be a positive integer

### Economic Constraints
- economicMaximum >= economicMinimum
- All MW values (economicMaximum, economicMinimum, actualOutput) must be non-negative
- actualOutput must be within reasonable bounds (10% tolerance) of [economicMinimum, economicMaximum]
- minimumRunTime must be positive

### Datetime Validation
- commitmentEnd must be after commitmentStart
- Both datetime fields must be valid ISO 8601 format

## Testing

Run the complete test suite:

```bash
pytest sourcing/scraping/miso/rsg_commitments/tests/ -v
```

Run specific test classes:

```bash
# Test candidate generation
pytest sourcing/scraping/miso/rsg_commitments/tests/test_scraper_miso_rsg_commitments.py::TestCandidateGeneration -v

# Test validation rules
pytest sourcing/scraping/miso/rsg_commitments/tests/test_scraper_miso_rsg_commitments.py::TestCommitmentRecordValidation -v

# Test edge cases
pytest sourcing/scraping/miso/rsg_commitments/tests/test_scraper_miso_rsg_commitments.py::TestEdgeCases -v
```

Run with coverage:

```bash
pytest sourcing/scraping/miso/rsg_commitments/tests/ --cov=sourcing.scraping.miso.rsg_commitments --cov-report=html
```

### Test Coverage

The test suite includes:

- **Initialization tests:** Validate filter parameters
- **Candidate generation:** Test with/without filters, metadata, identifiers
- **Content collection:** HTTP requests, error handling, query parameters
- **Structure validation:** Response format, metadata validation
- **Record validation:** All validation rules (30+ test cases)
  - Missing fields
  - Invalid enum values
  - Numeric field validation
  - Economic constraint validation
  - Datetime logic validation
  - All valid resource types and commitment reasons
- **Edge cases:** Empty lists, large datasets, zero/large values
- **Integration tests:** Full collection flow, duplicate detection, validation failures

**Total test cases:** 100+

## Scheduling

For production deployments, schedule the scraper to run every 5 minutes using cron or a task scheduler:

```bash
# Cron example (every 5 minutes)
*/5 * * * * /path/to/venv/bin/python /path/to/scraper_miso_rsg_commitments.py --s3-bucket prod-bucket --environment prod
```

Or use a scheduler like Apache Airflow:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag = DAG(
    'miso_rsg_commitments',
    default_args={'retries': 2},
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False
)

collect_task = BashOperator(
    task_id='collect_rsg_commitments',
    bash_command='python scraper_miso_rsg_commitments.py --s3-bucket prod-bucket --environment prod',
    dag=dag
)
```

## Monitoring

Key metrics to monitor:

- **Collection success rate:** Should be >99%
- **Duplicate rate:** Varies based on update frequency (expect ~50-70% duplicates for 5-minute polling)
- **Data volume:** Typically hundreds to thousands of commitments per snapshot
- **Response time:** API typically responds in <2 seconds
- **Validation failures:** Should be rare (<1%)

## Troubleshooting

### Connection Errors

```
Failed to fetch RSG commitments: Connection timeout
```

**Solution:** Check network connectivity, increase timeout, verify API endpoint is accessible.

### Redis Connection Failed

```
Failed to connect to Redis: Connection refused
```

**Solution:** Verify Redis is running and accessible at the specified host/port.

### AWS Credentials Error

```
Unable to locate credentials
```

**Solution:** Configure AWS credentials via `aws configure` or set environment variables.

### Validation Failures

Check logs for specific validation errors:

```bash
python scraper_miso_rsg_commitments.py --log-level DEBUG ...
```

Common validation issues:
- API returning incomplete data (temporary API issue)
- Unexpected enum values (API schema change)
- Economic constraints violated (data quality issue)

## Architecture

### Class Structure

```
MisoRSGCommitmentsCollector (extends BaseCollector)
├── generate_candidates() - Creates single snapshot candidate
├── collect_content() - Fetches data via HTTP GET
├── validate_content() - Validates response structure and data quality
└── _validate_commitment_record() - Validates individual commitment records
```

### Dependencies

- `sourcing.infrastructure.collection_framework` - Base collection infrastructure
- `sourcing.infrastructure.hash_registry` - Redis-based deduplication
- `sourcing.infrastructure.kafka_utils` - Kafka notification publishing

## API Documentation

Official MISO API documentation: https://www.misoenergy.org/markets-and-operations/real-time-operations/

## License

Internal use only - Enverus/Acuity Platform

## Version History

- **1.0.0** (2025-12-05)
  - Initial implementation
  - Real-time snapshot collection
  - Optional filtering support
  - Comprehensive validation rules
  - Full test coverage (100+ tests)

## Contact

For issues or questions, contact the Data Engineering team.
