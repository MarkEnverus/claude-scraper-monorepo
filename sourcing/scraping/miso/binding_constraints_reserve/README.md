# MISO Binding Constraints Reserve Scraper

Collects reserve-related binding constraints data from MISO's public API for ancillary services analysis.

## Overview

**Data Source**: MISO (Midcontinent Independent System Operator)
**API Endpoint**: `https://public-api.misoenergy.org/api/BindingConstraints/Reserve`
**Update Frequency**: Real-time (every 5 minutes)
**Authentication**: None (public API)
**Data Format**: JSON

## What This Scraper Collects

Reserve-related binding constraints provide critical insights into:
- Ancillary services market dynamics
- Reserve capacity limitations (spinning, non-spinning, regulation)
- Power system operational reserve requirements
- Market pricing impacts for reserve services

### Reserve Types Tracked
- **SPIN**: Spinning Reserve
- **NSPIN**: Non-Spinning Reserve
- **REG_UP**: Regulation Up
- **REG_DOWN**: Regulation Down
- **SUPPL**: Supplemental Reserve

## Data Structure

Each response contains:
- `RefId`: Timestamp of data snapshot
- `Constraint`: Array of reserve constraint objects

### Constraint Object Fields
- `Name`: Unique identifier for reserve constraint
- `Period`: Timestamp of constraint measurement
- `Price`: Shadow price for reserve services ($/MW)
- `ReserveType`: Type of reserve service (SPIN, NSPIN, REG_UP, REG_DOWN, SUPPL)
- `Direction`: Constraint direction (UP, DOWN, BOTH)
- `Quantity`: Reserve capacity affected (MW)
- `OVERRIDE`: Special operational condition flag
- `BP1`, `PC1`, `BP2`, `PC2`: Breakpoint and percentage parameters

## Architecture

Uses the `BaseCollector` framework with:
- **Hash-based deduplication**: Redis stores content hashes to prevent duplicate storage
- **S3 storage**: Compressed JSON files with date partitioning
- **Kafka notifications**: Optional downstream processing triggers
- **Comprehensive validation**: Type checking and business rule validation

## Installation

### Prerequisites

1. **Python 3.11+**
2. **Redis** (for deduplication)
3. **AWS credentials** configured (for S3 access)

### Dependencies

```bash
pip install boto3 redis requests click
```

## Usage

### Basic Usage

```bash
python scraper_miso_binding_constraints_reserve.py \
    --s3-bucket your-bucket-name \
    --redis-host localhost \
    --redis-port 6379
```

### With AWS Profile (LocalStack)

```bash
python scraper_miso_binding_constraints_reserve.py \
    --s3-bucket scraper-testing \
    --aws-profile localstack \
    --redis-host localhost
```

### With Kafka Notifications

```bash
export KAFKA_CONNECTION_STRING="user:password@kafka-broker:9092"

python scraper_miso_binding_constraints_reserve.py \
    --s3-bucket your-bucket-name \
    --kafka-connection-string "$KAFKA_CONNECTION_STRING"
```

### Command Line Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `--s3-bucket` | `S3_BUCKET` | (required) | S3 bucket for storing data |
| `--aws-profile` | `AWS_PROFILE` | None | AWS profile name |
| `--environment` | - | `dev` | Environment (dev/staging/prod) |
| `--redis-host` | `REDIS_HOST` | `localhost` | Redis server host |
| `--redis-port` | `REDIS_PORT` | `6379` | Redis server port |
| `--redis-db` | `REDIS_DB` | `0` | Redis database number |
| `--log-level` | - | `INFO` | Logging level (DEBUG/INFO/WARNING/ERROR) |
| `--kafka-connection-string` | `KAFKA_CONNECTION_STRING` | None | Kafka connection string |

## Storage Structure

Data is stored in S3 with the following structure:

```
s3://your-bucket/sourcing/
└── miso_binding_constraints_reserve/
    └── YYYY-MM-DD/
        └── binding_constraints_reserve_YYYYMMDD_HHMM.json.gz
```

Example:
```
s3://scraper-testing/sourcing/
└── miso_binding_constraints_reserve/
    └── 2025-12-05/
        └── binding_constraints_reserve_20251205_0935.json.gz
```

## Testing

### Run All Tests

```bash
pytest sourcing/scraping/miso/binding_constraints_reserve/tests/ -v
```

### Run Specific Test Classes

```bash
# Test candidate generation
pytest sourcing/scraping/miso/binding_constraints_reserve/tests/test_scraper_miso_binding_constraints_reserve.py::TestCandidateGeneration -v

# Test content validation
pytest sourcing/scraping/miso/binding_constraints_reserve/tests/test_scraper_miso_binding_constraints_reserve.py::TestContentValidation -v

# Test integration
pytest sourcing/scraping/miso/binding_constraints_reserve/tests/test_scraper_miso_binding_constraints_reserve.py::TestIntegration -v
```

### Test Coverage

```bash
pytest sourcing/scraping/miso/binding_constraints_reserve/tests/ --cov=sourcing.scraping.miso.binding_constraints_reserve --cov-report=html
```

## Validation Rules

The scraper validates:

1. **Response structure**: Must be a JSON object with `RefId` and `Constraint` fields
2. **RefId**: Must be a non-empty string timestamp
3. **Constraint array**: Must be a list (can be empty for no active constraints)
4. **Required fields**: All constraint objects must have all 11 required fields
5. **Name validation**: Non-empty string
6. **Period validation**: Non-empty string timestamp
7. **Price validation**: Numeric value ($/MW)
8. **ReserveType**: Should be one of SPIN, NSPIN, REG_UP, REG_DOWN, SUPPL (logs warning for unexpected types)
9. **Direction**: Should be one of UP, DOWN, BOTH (logs warning for unexpected values)
10. **Quantity validation**: Numeric value (MW)
11. **OVERRIDE validation**: Boolean or integer (0/1)
12. **Breakpoint validation**: BP1, PC1, BP2, PC2 must be numeric or null

## Deduplication Strategy

Uses **content-based hashing**:
1. Computes SHA-256 hash of response content
2. Checks Redis for existing hash
3. If hash exists, skips storage (duplicate)
4. If new, stores to S3 and registers hash in Redis
5. Hash TTL: 7 days (prevents indefinite growth)

## Error Handling

The scraper handles:
- **Network timeouts**: Configurable timeout (default 30s)
- **HTTP errors**: 4xx/5xx responses logged with details
- **Invalid JSON**: Validation fails, error logged
- **Redis connection failures**: Fails fast with clear error
- **S3 upload failures**: Retries with exponential backoff

## Monitoring

Check collection results:
```python
{
    "total_candidates": 1,
    "collected": 1,
    "skipped_duplicate": 0,
    "failed": 0,
    "errors": []
}
```

## Reserve vs Transmission Constraints

### Transmission Constraints (Different API)
- Focus on physical power transmission limitations
- Measure power flow across transmission lines
- Impact locational marginal pricing (LMP)
- Concerned with power delivery infrastructure

### Reserve Constraints (This API)
- Focus on power system operational reserves
- Measure available generation capacity for emergencies
- Impact ancillary services market pricing
- Concerned with generation capability and system stability

## Scheduling

### Recommended Schedule

Since data updates every 5 minutes, recommended cron schedule:

```bash
# Run every 5 minutes
*/5 * * * * /path/to/python scraper_miso_binding_constraints_reserve.py --s3-bucket your-bucket
```

### Alternative: Continuous Monitoring

```bash
# Run in a loop with 5-minute sleep
while true; do
    python scraper_miso_binding_constraints_reserve.py --s3-bucket your-bucket
    sleep 300  # 5 minutes
done
```

## Troubleshooting

### Redis Connection Failed
```
Error: Failed to connect to Redis: Connection refused
```
**Solution**: Ensure Redis is running: `redis-server`

### S3 Access Denied
```
Error: botocore.exceptions.NoCredentialsError
```
**Solution**: Configure AWS credentials or use `--aws-profile`

### Empty Constraint Array
This is **not an error**. It means no reserve constraints are currently binding.

### API Returns 503
The MISO API may be temporarily unavailable. Implement retry logic or wait before retrying.

## Version Information

- **Infrastructure Version**: 1.3.0
- **Last Updated**: 2025-12-05
- **Python Version**: 3.11+

## Related Scrapers

- `scraper_miso_snapshot.py` - Grid status dashboard
- `scraper_miso_rt_exante_lmp.py` - Real-time LMP pricing
- `scraper_miso_da_exante_asm_mcp.py` - Day-ahead ancillary services pricing
- `scraper_miso_fuel_mix.py` - Fuel mix data

## References

- [MISO Public API Documentation](https://www.misoenergy.org/markets-and-operations/real-time-displays/)
- [Spec Document](/tmp/miso-pricing-specs/spec-public-bindingconstraints-reserve.md)
- [BaseCollector Framework](../../../infrastructure/collection_framework.py)

## Support

For issues or questions:
1. Check logs with `--log-level DEBUG`
2. Verify Redis and S3 connectivity
3. Review test fixtures in `tests/fixtures/`
4. Consult the spec document for API details
