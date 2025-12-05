# MISO Coordinated Transaction Scheduling (CTS) Scraper

Collects MISO-PJM interface forecasts from MISO's public Coordinated Transaction Scheduling API.

## Overview

The Coordinated Transaction Scheduling (CTS) API provides:
- 5-minute interval forecasted Locational Marginal Prices (LMPs) for MISO-PJM interface nodes
- LMP component breakdown (energy, congestion, losses)
- Transaction volumes and flow directions between MISO and PJM markets
- Real-time updates every 15 minutes with operating day forecasts

This data is critical for:
- Cross-market arbitrage opportunities between MISO and PJM
- Inter-regional trading strategy development
- Transmission constraint economics analysis
- Interface flow and pricing pattern analysis

## Data Source

- **API Endpoint**: `https://public-api.misoenergy.org/api/CoordinatedTransactionScheduling`
- **Method**: GET
- **Authentication**: None (public API)
- **Update Frequency**: Every 15 minutes
- **Collection Pattern**: Snapshot-based (real-time forecasts)

## Query Parameters

All query parameters are optional and filter the API response:

| Parameter | Type | Format | Description | Example |
|-----------|------|--------|-------------|---------|
| `date` | String | YYYY-MM-DD | Operating day | `2025-01-20` |
| `interval` | String | HH:MM | 5-minute market interval (00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55) | `14:35` |
| `node` | String | - | MISO-PJM interface node identifier | `MISO.PJM.INTERFACE1` |

## Response Structure

```json
{
  "data": [
    {
      "timestamp": "2025-01-20T09:00:00-05:00",
      "interval": "09:00",
      "node": "MISO.PJM.INTERFACE1",
      "forecastedLMP": {
        "total": 45.50,
        "components": {
          "energy": 42.00,
          "congestion": 2.50,
          "losses": 1.00
        },
        "direction": "export"
      },
      "transactionVolume": {
        "mwh": 250.5,
        "direction": "MISO_TO_PJM"
      }
    }
  ],
  "metadata": {
    "operatingDay": "2025-01-20",
    "retrievalTimestamp": "2025-01-20T09:12:00-05:00",
    "dataQuality": "FORECAST"
  }
}
```

### Field Descriptions

#### Data Object Fields

- `timestamp`: ISO 8601 timestamp in EST (start of 5-minute interval)
- `interval`: Time interval in HH:MM format
- `node`: MISO-PJM interface node identifier
- `forecastedLMP`: Forecasted Locational Marginal Price object
  - `total`: Total forecasted LMP in $/MWh
  - `components`: Price component breakdown
    - `energy`: Generation cost component
    - `congestion`: Transmission constraint cost
    - `losses`: Transmission loss cost
  - `direction`: Flow direction (`export`, `import`, or `neutral`)
- `transactionVolume`: Forecasted transaction volume
  - `mwh`: Volume in megawatt-hours
  - `direction`: Transaction flow (`MISO_TO_PJM`, `PJM_TO_MISO`, or `BALANCED`)

#### Metadata Fields

- `operatingDay`: Operating day in YYYY-MM-DD format
- `retrievalTimestamp`: ISO 8601 timestamp when data was retrieved
- `dataQuality`: Data quality indicator (`FORECAST`, `PRELIMINARY`, or `VALIDATED`)

## Expected Data Volume

- **Nodes**: Hundreds of MISO-PJM interface nodes
- **Intervals**: 288 five-minute intervals per day
- **Daily Forecasts**: Thousands of forecast records
- **File Sizes**: Typically 50KB-500KB per snapshot (compressed)

## Validation Rules

The scraper performs comprehensive validation:

1. **JSON Structure**: Valid JSON with required top-level fields
2. **LMP Arithmetic**: `total = energy + congestion + losses` (within 0.01 tolerance)
3. **Interval Format**: HH:MM with minutes in [00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
4. **LMP Direction**: Must be `export`, `import`, or `neutral`
5. **Transaction Direction**: Must be `MISO_TO_PJM`, `PJM_TO_MISO`, or `BALANCED`
6. **Numeric Values**: All price components must be valid numbers
7. **Volume Validation**: Transaction volumes must be non-negative
8. **Data Quality**: Must be `FORECAST`, `PRELIMINARY`, or `VALIDATED`
9. **Operating Day Match**: If date parameter provided, metadata.operatingDay must match

## Storage

- **Format**: JSON with gzip compression
- **S3 Path**: `s3://{bucket}/sourcing/miso_coordinated_transaction_scheduling/year={YYYY}/month={MM}/day={DD}/{identifier}.json.gz`
- **Partitioning**: Date-based (year/month/day)
- **Deduplication**: SHA256 hash-based via Redis

## Usage

### Prerequisites

1. **AWS Credentials**: Configure AWS credentials for S3 access
2. **Redis**: Running Redis instance for hash deduplication
3. **Python Dependencies**: Install required packages

```bash
pip install requests boto3 redis click
```

### Environment Variables

```bash
export S3_BUCKET=your-bucket-name
export REDIS_HOST=localhost
export REDIS_PORT=6379
export AWS_PROFILE=your-profile  # Optional
export KAFKA_CONNECTION_STRING=your-kafka-connection  # Optional
```

### Command-Line Examples

#### Collect Current Snapshot

```bash
python scraper_miso_coordinated_transaction_scheduling.py \
    --s3-bucket scraper-testing \
    --aws-profile localstack
```

#### Collect Specific Date

```bash
python scraper_miso_coordinated_transaction_scheduling.py \
    --s3-bucket scraper-testing \
    --date 2025-01-20
```

#### Collect Date Range

```bash
python scraper_miso_coordinated_transaction_scheduling.py \
    --s3-bucket scraper-testing \
    --start-date 2025-01-20 \
    --end-date 2025-01-22
```

#### Collect with Filters

```bash
# Specific interval
python scraper_miso_coordinated_transaction_scheduling.py \
    --s3-bucket scraper-testing \
    --date 2025-01-20 \
    --interval 14:35

# Specific node
python scraper_miso_coordinated_transaction_scheduling.py \
    --s3-bucket scraper-testing \
    --date 2025-01-20 \
    --node MISO.PJM.INTERFACE1

# All filters combined
python scraper_miso_coordinated_transaction_scheduling.py \
    --s3-bucket scraper-testing \
    --date 2025-01-20 \
    --interval 14:35 \
    --node MISO.PJM.INTERFACE1
```

#### Force Re-download

```bash
python scraper_miso_coordinated_transaction_scheduling.py \
    --s3-bucket scraper-testing \
    --date 2025-01-20 \
    --force
```

#### Enable Kafka Notifications

```bash
python scraper_miso_coordinated_transaction_scheduling.py \
    --s3-bucket scraper-testing \
    --kafka-connection-string "user:pass@broker1:9092,broker2:9092"
```

### Command-Line Options

```
Options:
  --s3-bucket TEXT                S3 bucket name for storing CTS data [required]
  --start-date YYYY-MM-DD        Start date for date range collection
  --end-date YYYY-MM-DD          End date for date range collection
  --date TEXT                    Specific operating day (YYYY-MM-DD)
  --interval TEXT                Specific 5-minute interval (HH:MM)
  --node TEXT                    Specific MISO-PJM interface node identifier
  --aws-profile TEXT             AWS profile to use
  --environment [dev|staging|prod]
                                 Environment name (default: dev)
  --redis-host TEXT              Redis host (default: localhost)
  --redis-port INTEGER           Redis port (default: 6379)
  --redis-db INTEGER             Redis database number (default: 0)
  --log-level [DEBUG|INFO|WARNING|ERROR]
                                 Logging level (default: INFO)
  --kafka-connection-string TEXT Kafka connection string (optional)
  --force                        Force re-download even if hash exists
  --help                         Show this message and exit
```

## Testing

### Run All Tests

```bash
pytest sourcing/scraping/miso/coordinated_transaction_scheduling/tests/ -v
```

### Run Specific Test Classes

```bash
# Test candidate generation
pytest sourcing/scraping/miso/coordinated_transaction_scheduling/tests/test_scraper_miso_coordinated_transaction_scheduling.py::TestCandidateGeneration -v

# Test content validation
pytest sourcing/scraping/miso/coordinated_transaction_scheduling/tests/test_scraper_miso_coordinated_transaction_scheduling.py::TestContentValidation -v

# Test interval validation
pytest sourcing/scraping/miso/coordinated_transaction_scheduling/tests/test_scraper_miso_coordinated_transaction_scheduling.py::TestIntervalValidation -v
```

### Test Coverage

```bash
pytest sourcing/scraping/miso/coordinated_transaction_scheduling/tests/ \
    --cov=sourcing.scraping.miso.coordinated_transaction_scheduling \
    --cov-report=html
```

## Architecture

### Class Structure

```
MisoCtsCollector (extends BaseCollector)
├── generate_candidates(**kwargs) -> List[DownloadCandidate]
│   └── Creates candidates for current snapshot or date range
├── collect_content(candidate) -> bytes
│   └── Fetches CTS data from API with query parameters
├── validate_content(content, candidate) -> bool
│   └── Validates JSON structure, LMP arithmetic, and data quality
└── _validate_interval_format(interval) -> bool
    └── Validates 5-minute interval format (HH:MM)
```

### Collection Flow

1. **Generate Candidates**: Create list of API requests to make
   - Current snapshot mode: Single request with no date parameter
   - Date mode: Single request with date parameter
   - Date range mode: Multiple requests, one per day

2. **Collect Content**: For each candidate
   - Make HTTP GET request with query parameters
   - Handle timeouts and HTTP errors
   - Return raw response bytes

3. **Validate Content**: For each response
   - Parse JSON structure
   - Validate LMP arithmetic (total = energy + congestion + losses)
   - Validate interval format (HH:MM with 5-minute increments)
   - Validate direction enums
   - Validate numeric fields and non-negative volumes
   - Check operating day matches date parameter if provided

4. **Deduplicate**: Check SHA256 hash against Redis
   - Skip if hash exists (unless --force)
   - Calculate hash of raw content

5. **Store**: Upload to S3
   - Gzip compression
   - Date-based partitioning (year/month/day)
   - Generate S3 version ID and ETag

6. **Notify**: Publish Kafka notification (optional)
   - Include S3 location, hash, metadata
   - Follow ScraperNotificationMessage pattern

7. **Register**: Store hash in Redis
   - TTL of 365 days
   - Include S3 path and metadata

## Error Handling

The scraper handles various error conditions:

- **HTTP Errors**: 404, 500, timeouts
- **Invalid JSON**: Malformed responses
- **Validation Failures**: Missing fields, incorrect types, arithmetic errors
- **Redis Connection Errors**: Deduplication failures
- **S3 Upload Errors**: Permission issues, network failures
- **Kafka Errors**: Non-fatal, logged but don't block collection

## Monitoring

Key metrics to monitor:

- **Collection Rate**: Candidates collected per run
- **Duplicate Rate**: Percentage of candidates skipped (hash exists)
- **Failure Rate**: Failed candidates per run
- **Validation Failures**: Invalid content detected
- **API Response Time**: Time to fetch from MISO API
- **S3 Upload Time**: Time to compress and upload
- **Data Volume**: Number of forecasts per snapshot
- **Data Quality**: Distribution of FORECAST/PRELIMINARY/VALIDATED

## Scheduling Recommendations

Based on 15-minute update frequency:

- **Production**: Run every 15 minutes (cron: `*/15 * * * *`)
- **Development**: Run on-demand or hourly
- **Historical Backfill**: Run once per day for previous days

Example cron entry:
```cron
# Collect MISO CTS data every 15 minutes
*/15 * * * * cd /path/to/repo && python sourcing/scraping/miso/coordinated_transaction_scheduling/scraper_miso_coordinated_transaction_scheduling.py --s3-bucket prod-bucket --environment prod
```

## Troubleshooting

### Issue: "Failed to connect to Redis"

**Solution**: Ensure Redis is running and accessible
```bash
redis-cli ping
# Should return: PONG
```

### Issue: "Failed to upload to S3"

**Solution**: Check AWS credentials and permissions
```bash
aws s3 ls s3://your-bucket-name/
# Should list bucket contents
```

### Issue: "Content validation failed"

**Solution**: Check API response format
```bash
curl "https://public-api.misoenergy.org/api/CoordinatedTransactionScheduling?date=2025-01-20" | jq
```

### Issue: "LMP arithmetic mismatch"

**Cause**: API returned inconsistent data (total != energy + congestion + losses)

**Solution**: This is a data quality issue from MISO. The scraper correctly rejects invalid data. Contact MISO if persistent.

## Infrastructure Version

- **Framework Version**: 1.3.0
- **Last Updated**: 2025-12-05
- **BaseCollector**: Extends `sourcing.infrastructure.collection_framework.BaseCollector`
- **Hash Registry**: Uses `sourcing.infrastructure.hash_registry.HashRegistry`

## Related Documentation

- [BaseCollector Framework](../../infrastructure/collection_framework.py)
- [Hash Registry](../../infrastructure/hash_registry.py)
- [Kafka Utils](../../infrastructure/kafka_utils.py)
- [MISO Public API Documentation](https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/)

## License

Internal use only. See repository LICENSE file.
