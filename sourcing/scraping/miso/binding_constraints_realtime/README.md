# MISO Real-Time Binding Constraints Scraper

Collects real-time transmission binding constraint data with shadow pricing from MISO's Public API.

## Overview

This scraper collects real-time transmission binding constraints that provide critical market insights into:
- Grid transmission congestion
- Power flow limitations
- Locational pricing impact via shadow prices
- Transmission system operational constraints

## Data Source

**Endpoint**: `https://public-api.misoenergy.org/api/BindingConstraints/RealTime`

**Authentication**: None required (public API)

**Update Frequency**: Real-time (5-minute updates)

**Historical Support**: No - real-time snapshot only

## Shadow Pricing

The `Price` field in each constraint record represents the **shadow price** ($/MWh) - the marginal economic value of relaxing a specific transmission constraint.

### Key Characteristics:
- Measures economic impact of transmission limitations
- Dynamically calculated in real-time
- Used to understand locational marginal pricing (LMP) components
- Higher shadow prices indicate more severe congestion with greater economic impact

### Pricing Calculation Factors:
1. Transmission line capacity
2. Current power flow
3. Alternative transmission routes
4. Locational supply and demand dynamics

## Data Format

### Response Structure

```json
{
  "RefId": "05-Dec-2025, 14:35 EST",
  "Constraint": [
    {
      "Name": "AMRN_3053_CNTY",
      "Period": "2025-12-05T14:35:00",
      "Price": 12.45,
      "OVERRIDE": 0,
      "CURVETYPE": "MW",
      "BP1": 100.0,
      "PC1": 95.0,
      "BP2": 120.0,
      "PC2": 98.0
    }
  ]
}
```

### Field Definitions

#### Root Level
- **RefId** (String): Timestamp of data retrieval (format: "DD-Mon-YYYY, HH:MM TimeZone")

#### Constraint Array Elements
- **Name** (String): Unique identifier for transmission constraint
- **Period** (String): Precise timestamp of constraint measurement (ISO-8601 format)
- **Price** (Number): Shadow price in $/MWh indicating economic impact of constraint
- **OVERRIDE** (Boolean): Indicates if standard constraint rules are overridden (0/1)
- **CURVETYPE** (String): Measurement type - "MW", "PERCENT", or blank
- **BP1/PC1/BP2/PC2** (Number): Breakpoint and percentage parameters for constraint activation

## Usage

### Basic Collection

```bash
# Collect current real-time snapshot
python scraper_miso_binding_constraints_realtime.py
```

### With S3 Storage

```bash
export S3_BUCKET=your-bucket
python scraper_miso_binding_constraints_realtime.py
```

### With Custom Configuration

```bash
python scraper_miso_binding_constraints_realtime.py \
    --s3-bucket your-bucket \
    --redis-host localhost \
    --redis-port 6379 \
    --environment prod \
    --log-level DEBUG
```

### Force Re-Collection

```bash
python scraper_miso_binding_constraints_realtime.py --force
```

## Command-Line Options

| Option | Description | Default | Environment Variable |
|--------|-------------|---------|---------------------|
| `--s3-bucket` | S3 bucket for data storage | None | `S3_BUCKET` |
| `--aws-profile` | AWS profile name | None | `AWS_PROFILE` |
| `--redis-host` | Redis host for deduplication | localhost | `REDIS_HOST` |
| `--redis-port` | Redis port | 6379 | `REDIS_PORT` |
| `--redis-db` | Redis database number | 0 | `REDIS_DB` |
| `--environment` | Environment (dev/staging/prod) | dev | - |
| `--force` | Force re-download of existing files | False | - |
| `--skip-hash-check` | Skip Redis hash-based deduplication | False | - |
| `--log-level` | Logging level (DEBUG/INFO/WARNING/ERROR) | INFO | - |

## Scheduling for Continuous Monitoring

Since this API provides only the current snapshot, schedule this scraper to run every 5 minutes for continuous monitoring:

### Cron Example

```cron
# Run every 5 minutes
*/5 * * * * cd /path/to/scraper && python scraper_miso_binding_constraints_realtime.py >> /var/log/miso-constraints.log 2>&1
```

### Airflow DAG Example

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag = DAG(
    'miso_binding_constraints_realtime',
    default_args={
        'owner': 'data-eng',
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    },
    description='Collect MISO real-time binding constraints',
    schedule_interval=timedelta(minutes=5),  # Every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

collect_task = BashOperator(
    task_id='collect_binding_constraints',
    bash_command='cd /path/to/scraper && python scraper_miso_binding_constraints_realtime.py',
    dag=dag,
)
```

## Data Volume

- **Typical Response Size**: < 50 KB
- **Constraint Count**: Varies (0-50+ active constraints at any time)
- **Empty Responses**: Valid - indicates no active binding constraints

## Error Handling

### HTTP 404
- **Meaning**: No binding constraints data available
- **Handling**: Returns empty valid response with note
- **Impact**: Not an error condition - normal when no constraints are active

### HTTP 429 (Rate Limit)
- **Meaning**: Too many requests to public API
- **Handling**: Raises ScrapingError
- **Recommendation**: Increase polling interval or add delays

### Network Errors
- **Handling**: Wrapped in ScrapingError with context
- **Recommendation**: Check network connectivity and endpoint availability

## Integration with BaseCollector Framework

This scraper extends the `BaseCollector` framework and provides:

- **generate_candidates()**: Creates single real-time snapshot candidate
- **collect_content()**: Fetches JSON data from public API
- **validate_content()**: Validates response structure and field types

### Framework Features Used:
- Redis-based hash deduplication
- S3 storage with timestamp partitioning
- Gzip compression
- Kafka notifications for downstream processing
- Comprehensive error handling and logging

## Testing

Run the test suite:

```bash
# Run all tests
pytest sourcing/scraping/miso/binding_constraints_realtime/tests/ -v

# Run specific test class
pytest sourcing/scraping/miso/binding_constraints_realtime/tests/test_scraper_miso_binding_constraints_realtime.py::TestDataCollection -v

# Run with coverage
pytest sourcing/scraping/miso/binding_constraints_realtime/tests/ --cov=sourcing.scraping.miso.binding_constraints_realtime --cov-report=html
```

## Test Fixtures

- **sample_response.json**: Sample response with 3 active binding constraints
- **empty_response.json**: Sample response with no active constraints

## Dependencies

- `requests`: HTTP client for API calls
- `boto3`: AWS S3 client for storage
- `redis`: Redis client for deduplication
- `click`: CLI framework
- `pytest`: Testing framework

## Version Information

- **Infrastructure Version**: 1.3.0
- **Last Updated**: 2025-12-05
- **Python**: 3.8+

## Related Scrapers

- `scraper_miso_rt_exante_lmp.py` - Real-Time Ex-Ante LMP data
- `scraper_miso_da_exante_lmp_api.py` - Day-Ahead Ex-Ante LMP data
- `scraper_miso_snapshot.py` - MISO market snapshot data

## Troubleshooting

### No data collected
- **Check**: Verify API endpoint is accessible
- **Check**: Confirm no active binding constraints (empty response is valid)
- **Action**: Test direct API call: `curl https://public-api.misoenergy.org/api/BindingConstraints/RealTime`

### Redis connection errors
- **Check**: Redis server is running: `redis-cli ping`
- **Check**: Correct host/port configuration
- **Action**: Update `--redis-host` and `--redis-port` parameters

### S3 upload errors
- **Check**: AWS credentials configured: `aws sts get-caller-identity`
- **Check**: S3 bucket exists and is accessible
- **Check**: Correct AWS profile specified
- **Action**: Verify IAM permissions for S3 write access

## Support

For issues or questions:
1. Check the test suite for usage examples
2. Review the spec document: `/tmp/miso-pricing-specs/spec-public-bindingconstraints.md`
3. Consult the BaseCollector framework documentation
4. Check MISO Public API documentation

## License

Internal use only - Proprietary
