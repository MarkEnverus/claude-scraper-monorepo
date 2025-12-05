# MISO Public Ancillary Services Market Clearing Price (MCP) Scraper

Collects real-time and day-ahead reserve market clearing prices from MISO's public API.

## Data Source

- **API Endpoint**: https://public-api.misoenergy.org/api/MarketPricing/GetAncillaryServicesMcp
- **Method**: GET
- **Format**: JSON
- **Update Frequency**: Every 5 minutes
- **Authentication**: None (public API)
- **Historical Support**: Limited (via datetime parameter)

## What is Ancillary Services MCP?

Market Clearing Price (MCP) for Ancillary Services represents the cost of procuring reserve capacity to maintain grid reliability. MISO operates several reserve markets:

- **Regulation**: Fast-responding reserves to maintain frequency (up/down within seconds)
- **Spin**: Spinning reserves synchronized to grid, available within 10 minutes
- **Supplemental**: Non-spinning reserves, available within 10 minutes
- **STR**: Short-term reserves for reliability
- **Ramp-up**: Capability to increase generation quickly
- **Ramp-down**: Capability to decrease generation quickly

Prices vary by product type, reserve zone (1-8), and market type (real-time vs day-ahead).

## Data Structure

The API returns JSON with the following structure:

```json
{
  "data": [
    {
      "datetime": "2025-12-05T10:30:00Z",
      "product": "Regulation",
      "zone": "Zone 1",
      "mcp": 5.23,
      "marketType": "real-time"
    }
  ],
  "metadata": {
    "totalRecords": 96,
    "updateTimestamp": "2025-12-05T10:35:00Z",
    "dataSource": "MISO Public Market Pricing"
  }
}
```

### Data Fields

- **datetime**: ISO 8601 timestamp of the market interval
- **product**: Reserve product name (Regulation, Spin, Supplemental, STR, Ramp-up, Ramp-down)
- **zone**: MISO reserve zone (Zone 1 through Zone 8)
- **mcp**: Market Clearing Price in $/MWh
- **marketType**: Market type (real-time or day-ahead)

### Metadata Fields

- **totalRecords**: Count of records in data array
- **updateTimestamp**: ISO 8601 timestamp of last data update
- **dataSource**: Source identifier (MISO Public Market Pricing)

## Query Parameters (Optional)

The API supports optional filtering parameters:

- **product**: Filter by reserve product name
  - Valid values: "Regulation", "Spin", "Supplemental", "STR", "Ramp-up", "Ramp-down"
- **zone**: Filter by reserve zone
  - Valid values: "Zone 1", "Zone 2", "Zone 3", "Zone 4", "Zone 5", "Zone 6", "Zone 7", "Zone 8"
- **datetime**: Filter by specific ISO 8601 datetime

## Expected Data Volume

Without filters (full snapshot):
- 6 products × 8 zones × 2 market types = 96 records per collection
- Updated every 5 minutes
- ~27,600 records per day (96 × 288 5-minute intervals)

With filters:
- Single product, single zone: 1-2 records per collection (RT and DA)
- Single product, all zones: 8-16 records per collection
- All products, single zone: 6-12 records per collection

## Validation Rules

The scraper validates:

1. **Required Fields**: `data`, `metadata`, `datetime`, `product`, `zone`, `mcp`, `marketType`
2. **Product Values**: Must be one of 6 valid products (case-sensitive)
3. **Zone Values**: Must be "Zone 1" through "Zone 8" (exact format)
4. **MCP Values**: Must be non-negative numbers
5. **Market Type**: Must be "real-time" or "day-ahead"
6. **Datetime Format**: Must be valid ISO 8601 format
7. **Record Count**: metadata.totalRecords must match data array length

## Storage

Data is stored to S3 with the following structure:

```
s3://{bucket}/sourcing/miso_public_asm_mcp/
  year=2025/
    month=12/
      day=05/
        public_asm_mcp_20251205_1030.json.gz
        public_asm_mcp_regulation_20251205_1035.json.gz
        public_asm_mcp_spin_zone1_20251205_1040.json.gz
```

Files are:
- Compressed with gzip
- Partitioned by date (year/month/day)
- Named with timestamp and any active filters

## Usage

### Basic Usage (All Products, All Zones)

```bash
python scraper_miso_public_asm_mcp.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack
```

### Filter by Product

```bash
# Collect only Regulation reserves
python scraper_miso_public_asm_mcp.py \
  --s3-bucket scraper-testing \
  --product Regulation

# Collect only Spinning reserves
python scraper_miso_public_asm_mcp.py \
  --s3-bucket scraper-testing \
  --product Spin
```

### Filter by Zone

```bash
# Collect Zone 1 prices
python scraper_miso_public_asm_mcp.py \
  --s3-bucket scraper-testing \
  --zone "Zone 1"

# Collect Zone 3 prices
python scraper_miso_public_asm_mcp.py \
  --s3-bucket scraper-testing \
  --zone "Zone 3"
```

### Filter by Product and Zone

```bash
# Collect Spin reserves for Zone 1
python scraper_miso_public_asm_mcp.py \
  --s3-bucket scraper-testing \
  --product Spin \
  --zone "Zone 1"
```

### Filter by Datetime

```bash
# Collect specific datetime
python scraper_miso_public_asm_mcp.py \
  --s3-bucket scraper-testing \
  --datetime "2025-12-05T10:30:00Z"

# Combine with product filter
python scraper_miso_public_asm_mcp.py \
  --s3-bucket scraper-testing \
  --product Regulation \
  --datetime "2025-12-05T10:30:00Z"
```

### With Environment Variables

```bash
export S3_BUCKET=scraper-testing
export AWS_PROFILE=localstack
export REDIS_HOST=localhost
export REDIS_PORT=6379

python scraper_miso_public_asm_mcp.py
```

### With Kafka Notifications

```bash
python scraper_miso_public_asm_mcp.py \
  --s3-bucket scraper-testing \
  --kafka-connection-string "user:pass@kafka-host:9092"
```

### With Debug Logging

```bash
python scraper_miso_public_asm_mcp.py \
  --s3-bucket scraper-testing \
  --log-level DEBUG
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest sourcing/scraping/miso/public_asm_mcp/tests/ -v

# Run with coverage
pytest sourcing/scraping/miso/public_asm_mcp/tests/ -v --cov=sourcing.scraping.miso.public_asm_mcp

# Run specific test
pytest sourcing/scraping/miso/public_asm_mcp/tests/test_scraper_miso_public_asm_mcp.py::TestMisoPublicASMMCPCollector::test_validate_content_valid -v
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | (required) | S3 bucket for data storage |
| `AWS_PROFILE` | (none) | AWS profile name for credentials |
| `REDIS_HOST` | localhost | Redis host for deduplication |
| `REDIS_PORT` | 6379 | Redis port |
| `REDIS_DB` | 0 | Redis database number |
| `KAFKA_CONNECTION_STRING` | (none) | Kafka connection for notifications |

## Infrastructure Dependencies

- **Redis**: Required for hash-based deduplication
- **S3**: Required for data storage
- **Kafka**: Optional for downstream notifications

## Use Cases

1. **Real-time Reserve Market Monitoring**: Track current reserve prices across all zones
2. **Reserve Cost Analysis**: Analyze reserve procurement costs by product and zone
3. **Market Type Comparison**: Compare real-time vs day-ahead reserve prices
4. **Zone-specific Analysis**: Focus on specific reserve zones
5. **Product-specific Tracking**: Monitor individual reserve products (e.g., Regulation only)
6. **Grid Reliability Insights**: Understand reserve price signals for grid conditions

## Architecture

This scraper follows the BaseCollector pattern:

- **BaseCollector**: Provides framework for deduplication, S3 storage, Kafka notifications
- **MisoPublicASMMCPCollector**: Implements MISO-specific collection logic
- **Hash Registry**: Redis-based deduplication using content hashes
- **S3 Storage**: Date-partitioned storage with gzip compression
- **Validation**: Comprehensive data integrity checks

## Version Information

- **Infrastructure Version**: 1.3.0
- **Last Updated**: 2025-12-05
- **Compatible with**: BaseCollector 1.3.0+

## Related Scrapers

- `scraper_miso_fuel_mix.py`: Real-time fuel mix data
- `scraper_miso_da_exante_asm_mcp.py`: Day-Ahead Ex-Ante ASM MCP (forecasted)
- `scraper_miso_rt_expost_asm_mcp.py`: Real-Time Ex-Post ASM MCP (actual)

## Notes

- API returns both real-time and day-ahead prices in single response
- Data updates every 5 minutes
- No authentication required (public API)
- Supports filtering to reduce data volume
- Deduplication prevents storing identical snapshots
- All prices in $/MWh
