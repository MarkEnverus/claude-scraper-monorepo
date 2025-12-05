# MISO Sub-Regional Binding Constraints Scraper

## Overview

This scraper collects sub-regional binding constraints data from MISO's Public API. Sub-regional binding constraints provide granular insights into localized transmission congestion, constraint utilization, and shadow pricing impacts at the sub-regional level.

**API Endpoint**: `https://public-api.misoenergy.org/api/BindingConstraints/SubRegional`

## Data Description

### What This Data Provides

Sub-regional binding constraints represent localized transmission system limitations that impact:
- Localized transmission congestion patterns
- Sub-regional grid transmission limitations
- Micro-level power flow constraints
- Geographic-specific congestion analysis
- Complementary view to real-time system-wide constraints

### Key Differences from Real-Time Binding Constraints

- **Granularity**: More detailed, sub-regional focus vs. system-wide constraints
- **Geographic Resolution**: Breaks down constraints by Region and Subregion
- **Use Case**: Ideal for regional market participants and localized grid analysis
- **Complementary**: Used alongside real-time constraints for comprehensive view

### Data Fields

Each constraint record includes:

- **Name**: Unique identifier for sub-regional constraint (e.g., "MINN_METRO_WEST_01")
- **Region**: Broader geographical region (e.g., "Minnesota", "Illinois")
- **Subregion**: More granular subdivision (e.g., "Metro West", "Urban East")
- **ConstraintType**: Classification of constraint
  - `TRANSMISSION_CAPACITY`: Line/corridor capacity limits
  - `VOLTAGE_LIMITATION`: Voltage stability constraints
  - `GENERATION_INTERCONNECTION`: Generation tie-in constraints
  - `LOAD_BALANCING`: Load distribution constraints
- **Period**: Timestamp of constraint measurement (ISO-8601 format)
- **Price**: Shadow price indicating economic impact ($/MWh)
  - Positive: Cost to relax constraint
  - Negative: Credit for reducing constraint
- **ConstraintValue**: Current measured value (MW or appropriate units)
- **ConstraintLimit**: Maximum allowable value (operational boundary)
- **Utilization**: Percentage of limit currently used (0-100%)
  - Calculated as: `(ConstraintValue / ConstraintLimit) × 100`
- **OVERRIDE**: Boolean indicating special operational override condition

### RefId Format

The `RefId` field provides the data timestamp:
- Format: "DD-Mon-YYYY - Interval HH:MM TZ"
- Example: "05-Dec-2025 - Interval 12:30 EST"
- Indicates precise moment of constraint snapshot

## Update Frequency

- **Real-time updates**: Every 5 minutes
- **API refresh rate**: 5-minute intervals
- **Recommended polling**: 1-5 minutes for near real-time monitoring

## Use Cases

### 1. Localized Congestion Analysis
Identify sub-regional transmission bottlenecks and micro-level grid constraints that may not be visible in system-wide constraint data.

### 2. Regional Market Strategy
Inform trading and generation dispatch decisions based on localized transmission pricing impacts and constraint patterns.

### 3. Grid Planning
Evaluate sub-regional transmission system performance and identify potential infrastructure needs for transmission upgrades.

### 4. Risk Management
Assess localized transmission constraint risks and support operational decision-making for generation and load management.

### 5. Price Impact Analysis
Understand how sub-regional constraints influence locational marginal pricing (LMP) and energy market dynamics.

## Constraint Utilization Categories

- **Low (0-30%)**: Ample capacity, minimal pricing impact
- **Moderate (30-70%)**: Increasing constraints, potential congestion emerging
- **High (70-90%)**: Significant constraints, high congestion risk
- **Critical (90-100%)**: Near maximum capacity, immediate operational attention required

## Installation & Setup

### Prerequisites

1. **Python 3.9+** with required packages:
   ```bash
   pip install boto3 click redis requests
   ```

2. **Redis** for hash-based deduplication:
   ```bash
   # macOS
   brew install redis
   brew services start redis

   # Ubuntu/Debian
   sudo apt-get install redis-server
   sudo systemctl start redis
   ```

3. **AWS credentials** configured for S3 access:
   ```bash
   aws configure
   # OR use AWS_PROFILE environment variable
   ```

### Environment Variables

No authentication required for this public API, but configure storage:

```bash
# S3 Storage
export S3_BUCKET=your-bucket-name
export AWS_PROFILE=your-aws-profile  # Optional

# Redis Configuration
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB=0
```

## Usage

### Basic Collection

Collect a single snapshot:
```bash
python scraper_miso_binding_constraints_subregional.py \
    --start-date 2025-12-05T12:00:00 \
    --end-date 2025-12-05T12:00:00
```

### Time Range Collection

Collect data for one hour (12 snapshots at 5-minute intervals):
```bash
python scraper_miso_binding_constraints_subregional.py \
    --start-date 2025-12-05T12:00:00 \
    --end-date 2025-12-05T13:00:00
```

### Full Day Collection

Collect full day of constraint data:
```bash
python scraper_miso_binding_constraints_subregional.py \
    --start-date 2025-12-05 \
    --end-date 2025-12-06
```

### Production Collection

```bash
python scraper_miso_binding_constraints_subregional.py \
    --start-date 2025-12-05 \
    --end-date 2025-12-06 \
    --environment prod \
    --s3-bucket your-prod-bucket \
    --log-level INFO
```

### Force Re-download

Force re-download even if already collected:
```bash
python scraper_miso_binding_constraints_subregional.py \
    --start-date 2025-12-05T12:00:00 \
    --end-date 2025-12-05T12:30:00 \
    --force
```

## Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--start-date` | Start date/time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS) | Required |
| `--end-date` | End date/time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS) | Required |
| `--s3-bucket` | S3 bucket for storage | `$S3_BUCKET` |
| `--aws-profile` | AWS profile name | `$AWS_PROFILE` |
| `--redis-host` | Redis host | `localhost` |
| `--redis-port` | Redis port | `6379` |
| `--redis-db` | Redis database number | `0` |
| `--environment` | Environment (dev/staging/prod) | `dev` |
| `--force` | Force re-download existing files | `False` |
| `--skip-hash-check` | Skip Redis deduplication | `False` |
| `--log-level` | Logging level (DEBUG/INFO/WARNING/ERROR) | `INFO` |

## Data Storage

### S3 Structure

```
s3://your-bucket/sourcing/
└── miso_binding_constraints_subregional/
    └── dev/  # or staging/prod
        └── date=2025-12-05/
            ├── binding_constraints_subregional_20251205_120000.json.gz
            ├── binding_constraints_subregional_20251205_120500.json.gz
            ├── binding_constraints_subregional_20251205_121000.json.gz
            └── ...
```

### File Format

Each JSON file contains:
```json
{
  "data": {
    "RefId": "05-Dec-2025 - Interval 12:30 EST",
    "SubRegionalConstraints": [
      {
        "Name": "MINN_METRO_WEST_01",
        "Region": "Minnesota",
        "Subregion": "Metro West",
        "ConstraintType": "TRANSMISSION_CAPACITY",
        "Period": "2025-12-05T12:30:00Z",
        "Price": 12.50,
        "ConstraintValue": 450.0,
        "ConstraintLimit": 500.0,
        "Utilization": 90.0,
        "OVERRIDE": false
      }
    ]
  },
  "collection_metadata": {
    "collected_at": "2025-12-05T12:35:45",
    "source_url": "https://public-api.misoenergy.org/api/BindingConstraints/SubRegional",
    "data_type": "binding_constraints_subregional",
    "source": "miso"
  }
}
```

## Testing

### Run All Tests

```bash
pytest sourcing/scraping/miso/binding_constraints_realtime/tests/test_scraper_miso_binding_constraints_subregional.py -v
```

### Run Specific Test

```bash
pytest sourcing/scraping/miso/binding_constraints_realtime/tests/test_scraper_miso_binding_constraints_subregional.py::TestMisoBindingConstraintsSubregionalCollector::test_collect_content_success -v
```

### Test Coverage

```bash
pytest sourcing/scraping/miso/binding_constraints_realtime/tests/test_scraper_miso_binding_constraints_subregional.py --cov=sourcing.scraping.miso.binding_constraints_realtime.scraper_miso_binding_constraints_subregional --cov-report=html
```

## Error Handling

The scraper handles various error scenarios:

### HTTP Errors
- **404**: Data not available (logged as warning)
- **429**: Rate limit exceeded (implements retry logic)
- **503**: Service unavailable (retries with backoff)

### Data Validation
- Missing required fields → Validation failure
- Invalid data types → Validation failure
- Empty constraints array → Valid (no active constraints)
- Utilization out of range (0-100%) → Warning logged but accepted

### Network Issues
- Timeouts → Retry with exponential backoff
- Connection errors → Logged and skipped

## Monitoring & Logging

### Log Levels

- **DEBUG**: Detailed request/response information
- **INFO**: Collection progress and statistics
- **WARNING**: Non-fatal issues (404s, empty data)
- **ERROR**: Failed collections, validation errors

### Collection Metrics

Each run logs:
- Total candidates generated
- Files downloaded
- Files skipped (already exists)
- Files failed (errors)
- Constraint statistics (type counts, high utilization, overrides)

Example log output:
```
INFO - Starting MISO Sub-Regional Binding Constraints collection
INFO - Connected to Redis at localhost:6379/0
INFO - Using S3 bucket: your-bucket
INFO - Generated candidate for timestamp: 20251205_120000
INFO - Successfully collected Sub-Regional Binding Constraints data. RefId: 05-Dec-2025 - Interval 12:30 EST, Constraints: 12 active
INFO - Constraint breakdown: {'TRANSMISSION_CAPACITY': 8, 'VOLTAGE_LIMITATION': 3, 'GENERATION_INTERCONNECTION': 1}, High utilization (≥90%): 4, Overrides: 1
INFO - Collection complete: files_downloaded=12, files_skipped=0, files_failed=0
```

## Data Quality & Validation

### Automatic Validation

The scraper performs comprehensive validation:

1. **Structure validation**: Required fields present
2. **Type validation**: Correct data types (strings, numbers, booleans)
3. **Numeric validation**: Price, ConstraintValue, ConstraintLimit, Utilization are numeric
4. **Range validation**: Utilization within 0-100% (warning if exceeded)
5. **Boolean validation**: OVERRIDE is boolean or 0/1

### Manual Verification

Verify collected data:
```bash
# Check latest file
aws s3 cp s3://your-bucket/sourcing/miso_binding_constraints_subregional/dev/date=2025-12-05/binding_constraints_subregional_20251205_120000.json.gz - | gunzip | jq .

# Count constraints per file
aws s3 cp s3://your-bucket/sourcing/miso_binding_constraints_subregional/dev/date=2025-12-05/binding_constraints_subregional_20251205_120000.json.gz - | gunzip | jq '.data.SubRegionalConstraints | length'

# Check high utilization constraints
aws s3 cp s3://your-bucket/sourcing/miso_binding_constraints_subregional/dev/date=2025-12-05/binding_constraints_subregional_20251205_120000.json.gz - | gunzip | jq '.data.SubRegionalConstraints[] | select(.Utilization >= 90)'
```

## Deduplication Strategy

Uses Redis-based hash deduplication:

1. Content hash calculated from constraint data
2. Hash checked against Redis registry
3. Duplicate content skipped (logged)
4. New content stored to S3

Skip deduplication if needed:
```bash
python scraper_miso_binding_constraints_subregional.py \
    --start-date 2025-12-05 \
    --end-date 2025-12-06 \
    --skip-hash-check
```

## Performance Considerations

- **Lightweight API**: Response typically < 50KB
- **Fast collection**: ~200-500ms per request
- **5-minute intervals**: 12 snapshots/hour, 288 snapshots/day
- **Daily data volume**: ~10-15MB uncompressed, ~2-3MB gzipped

## Troubleshooting

### Redis Connection Failure
```bash
# Check Redis is running
redis-cli ping
# Should return: PONG

# Start Redis if not running
brew services start redis  # macOS
sudo systemctl start redis  # Linux
```

### S3 Access Denied
```bash
# Verify AWS credentials
aws sts get-caller-identity

# Check S3 bucket access
aws s3 ls s3://your-bucket/

# Verify IAM permissions for S3 PutObject
```

### Empty Constraints Response

This is normal and valid - when the API returns:
```json
{
  "RefId": "05-Dec-2025 - Interval 12:30 EST",
  "SubRegionalConstraints": []
}
```

This indicates no active sub-regional constraints at that moment. The scraper will log this and continue.

### Rate Limiting

If encountering 429 errors:
- Reduce collection frequency
- Add delays between requests
- Contact MISO for rate limit increases

## Infrastructure

- **Framework**: BaseCollector (collection_framework.py v1.3.0)
- **Hash Registry**: Redis-based (hash_registry.py)
- **Storage**: S3 with gzip compression
- **Logging**: JSON structured logging (logging_json.py)

## Version History

- **v1.0.0** (2025-12-05): Initial release
  - Real-time sub-regional constraints collection
  - 5-minute update frequency
  - Comprehensive validation and error handling
  - S3 storage with date partitioning
  - Redis deduplication

## References

- [MISO Public API Documentation](https://www.misoenergy.org/markets-and-operations/real-time-displays/)
- [Sub-Regional Binding Constraints Specification](/tmp/miso-pricing-specs/spec-public-bindingconstraints-subregional.md)
- [Collection Framework Documentation](../../../infrastructure/collection_framework.py)

## Support

For issues or questions:
1. Check logs for detailed error messages
2. Verify environment configuration
3. Review specification document
4. Contact data engineering team

---

Generated with Claude Code
