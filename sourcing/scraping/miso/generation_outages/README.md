# MISO Generation Outages Scraper

Collects generation outage data from MISO's public API.

## Overview

This scraper collects generation outage data for the MISO electrical grid, including:
- **Unplanned outages**: Unexpected generation unit shutdowns
- **Planned outages**: Scheduled maintenance or known generation reduction
- **Forced outages**: Immediate, unscheduled outages due to technical failures
- **Derated capacity**: Generation operating below full potential

The API provides a 10-day rolling window (±5 days from current date) of daily outage data.

## API Details

- **Endpoint**: `https://public-api.misoenergy.org/api/GenerationOutages/GetGenerationOutagesPlusMinusFiveDays`
- **Method**: GET
- **Authentication**: None (public API)
- **Response Format**: JSON
- **Update Frequency**: Daily
- **Data Window**: 10 days (±5 days from current date)

## Data Structure

### Response Fields

- **RefId**: Reference identifier with total outage information
  - Format: "DD-Mon-YYYY - Total Outage Megawatts: X,XXX"
  - Example: "05-Dec-2025 - Total Outage Megawatts: 40,964"

- **Days**: Array of daily outage objects
  - **OutageDate**: Full timestamp of the outage date (ISO 8601 format)
  - **OutageMonthDay**: Abbreviated month and day (e.g., "Dec-1")
  - **Unplanned**: Megawatts of unplanned generation outages
  - **Planned**: Megawatts of planned generation outages
  - **Forced**: Megawatts of forced generation outages
  - **Derated**: Megawatts of derated capacity

### Example Response

```json
{
  "RefId": "05-Dec-2025 - Total Outage Megawatts: 40,964",
  "Days": [
    {
      "OutageDate": "2025-12-01T00:00:00Z",
      "OutageMonthDay": "Dec-1",
      "Unplanned": 5000,
      "Planned": 3000,
      "Forced": 2500,
      "Derated": 1500
    }
  ]
}
```

## Usage

### Prerequisites

1. **Redis**: Running Redis instance for hash deduplication
2. **S3 Bucket**: AWS S3 bucket for data storage
3. **AWS Credentials**: Configured AWS profile or credentials

### Running the Scraper

```bash
# Basic usage with LocalStack
python scraper_miso_generation_outages.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack

# With production AWS
python scraper_miso_generation_outages.py \
  --s3-bucket production-bucket \
  --environment prod

# With Kafka notifications
export KAFKA_CONNECTION_STRING="username:password@broker1:9092,broker2:9092/topic-name"
python scraper_miso_generation_outages.py \
  --s3-bucket scraper-testing \
  --aws-profile localstack
```

### Command-Line Options

- `--s3-bucket`: S3 bucket name (required, or set S3_BUCKET env var)
- `--aws-profile`: AWS profile name (optional, for LocalStack or AWS)
- `--environment`: Environment name (dev/staging/prod, default: dev)
- `--redis-host`: Redis host (default: localhost)
- `--redis-port`: Redis port (default: 6379)
- `--redis-db`: Redis database number (default: 0)
- `--log-level`: Logging level (DEBUG/INFO/WARNING/ERROR, default: INFO)
- `--kafka-connection-string`: Kafka connection string for notifications (optional)

### Environment Variables

- `S3_BUCKET`: S3 bucket name
- `AWS_PROFILE`: AWS profile to use
- `REDIS_HOST`: Redis host
- `REDIS_PORT`: Redis port
- `REDIS_DB`: Redis database number
- `KAFKA_CONNECTION_STRING`: Kafka connection string

## Features

- **HTTP Collection**: Uses BaseCollector framework for reliable data collection
- **Hash Deduplication**: Redis-based content hashing prevents duplicate storage
- **S3 Storage**: Automatic upload with date partitioning and gzip compression
- **Kafka Notifications**: Optional downstream processing notifications
- **Comprehensive Validation**: Validates JSON structure and required fields
- **Error Handling**: Robust error handling with detailed logging

## Storage Format

Data is stored in S3 with the following structure:

```
s3://{bucket}/sourcing/miso_generation_outages/
  year=2025/
    month=12/
      day=05/
        generation_outages_20251205_1400.json.gz
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest sourcing/scraping/miso/generation_outages/tests/ -v

# Run with coverage
pytest sourcing/scraping/miso/generation_outages/tests/ --cov=sourcing.scraping.miso.generation_outages --cov-report=html

# Run specific test class
pytest sourcing/scraping/miso/generation_outages/tests/test_scraper_miso_generation_outages.py::TestContentValidation -v
```

## Business Value

### Use Cases

1. **Grid Capacity Analysis**: Understand available generation capacity
2. **Market Price Prediction**: Outages impact electricity prices
3. **Reliability Assessment**: Track grid stability and generation reliability
4. **Forecasting**: Day-ahead market price prediction
5. **Research**: Analyze outage patterns and trends

### Typical Users

- Energy trading firms
- Utility companies
- Grid reliability analysts
- Academic and research institutions
- Energy policy makers

## Outage Types Explained

### Unplanned Outages
- Unexpected generation unit shutdowns
- Not scheduled in advance
- Typically due to unexpected equipment failure
- Immediate impact on grid generation capacity

### Planned Outages
- Scheduled generation unit unavailability
- Predetermined and communicated in advance
- Typically for maintenance, upgrades, or inspections
- Minimal grid disruption due to advance planning

### Forced Outages
- Immediate, unplanned generation unit shutdown
- Occurs without prior warning
- Requires immediate action
- Significant grid capacity impact

### Derated Generation
- Partial generation capacity reduction
- Unit operates below full potential
- Partial rather than complete shutdown
- Allows continued, though reduced, generation

## Architecture

```
MisoGenerationOutagesCollector (BaseCollector)
    ├── generate_candidates() - Creates single candidate for 10-day window
    ├── collect_content() - Fetches JSON from MISO API
    ├── validate_content() - Validates JSON structure and data integrity
    └── run_collection() - Orchestrates collection workflow
        ├── Hash deduplication (Redis)
        ├── S3 upload (gzip compressed)
        ├── Kafka notification (optional)
        └── Error handling & logging
```

## Monitoring

Key metrics to monitor:
- Collection success rate
- Duplicate rate (should be high for frequent collections)
- API response time
- Data validation failures
- S3 upload success rate

## Troubleshooting

### Redis Connection Failed
- Verify Redis is running: `redis-cli ping`
- Check Redis host/port configuration

### S3 Upload Failed
- Verify AWS credentials are configured
- Check S3 bucket exists and has proper permissions
- For LocalStack: ensure profile is set correctly

### API Returns Empty Days Array
- This may indicate API maintenance or temporary outage
- Check MISO's status page
- Retry after a few minutes

### Validation Failures
- Check API response structure hasn't changed
- Review MISO API documentation for updates
- Examine raw response in logs (use --log-level DEBUG)

## Version Information

- **Infrastructure Version**: 1.3.0
- **Last Updated**: 2025-12-05
- **Python Version**: 3.11+

## Related Documentation

- [MISO Public API Documentation](https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/)
- [BaseCollector Framework](../../infrastructure/collection_framework.py)
- [Hash Registry Documentation](../../infrastructure/hash_registry.py)
