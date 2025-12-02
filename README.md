# Scraper Monorepo

Production-ready data collection framework for energy market data scrapers.

## Overview

This monorepo provides a standardized infrastructure for building data scrapers that collect, deduplicate, and store data from various energy market APIs and sources. All scrapers use a common collection framework with Redis-based deduplication, S3 storage, and optional Kafka notifications.

## Project Structure

```
sourcing/
├── infrastructure/          # Shared collection framework
│   ├── collection_framework.py    # BaseCollector abstract class
│   ├── hash_registry.py           # Redis deduplication
│   └── logging_json.py            # Structured logging
│
└── scraping/               # Data source scrapers
    └── {dataProvider}/     # e.g., miso, ercot, pjm
        └── {dataSource}/   # e.g., wind_forecast, load, price
            ├── scraper_{provider}_{source}.py
            ├── README.md                      # Usage documentation
            ├── README_INTEGRATION_TEST.md     # Test results
            └── tests/                         # Test suite
                ├── test_scraper_*.py
                └── fixtures/
```

**Naming Convention:** `scraping/{dataProvider}/{dataSource}/`
- **dataProvider**: MISO, ERCOT, PJM, etc.
- **dataSource**: wind_forecast, load, price, etc.

## Infrastructure Components

### Collection Framework
**BaseCollector** (`sourcing/infrastructure/collection_framework.py`)
- Abstract base class for all scrapers
- Implements common patterns: candidate generation, collection, validation
- Built-in S3 upload with gzip compression
- Automatic date partitioning: `year={YYYY}/month={MM}/day={DD}/`
- Redis hash-based deduplication
- Optional Kafka notifications

### Hash Registry
**HashRegistry** (`sourcing/infrastructure/hash_registry.py`)
- SHA-256 content hashing
- Redis-based duplicate detection
- Configurable TTL (default 365 days)
- Key format: `hash:{environment}:{dgroup}:{sha256}`

### Storage
- **S3**: Date-partitioned storage with gzip compression
- **Redis**: Deduplication and metadata storage
- **Kafka** (optional): Downstream processing notifications

## Testing Infrastructure

### Local Development Stack
- **LocalStack** (Docker) - AWS S3 emulation on port 4566
- **Kafka** (Docker) - Event streaming broker
- **Redis** (localhost:6379) - Hash deduplication
- **Pytest** - Test framework with fixtures and mocks

### Integration Testing
Each scraper includes:
- Comprehensive unit tests (`tests/test_scraper_*.py`)
- Test fixtures for sample data
- Integration test results documentation (`README_INTEGRATION_TEST.md`)

## Current Scrapers

### MISO Wind Forecast
**Location:** `sourcing/scraping/miso/wind_forecast/`

**Data Source:** Wind power forecast from MISO public API
**Update Frequency:** Real-time (5-minute intervals)
**Infrastructure Version:** 1.3.0

**Features:**
- ✅ HTTP collection from public API
- ✅ JSON validation
- ✅ Redis deduplication
- ✅ S3 storage with gzip
- ✅ Kafka notifications
- ✅ Comprehensive test suite

**Documentation:**
- [Usage Guide](sourcing/scraping/miso/wind_forecast/README.md)
- [Integration Test Results](sourcing/scraping/miso/wind_forecast/README_INTEGRATION_TEST.md)

## Getting Started

### Prerequisites

```bash
# Python 3.10+ required
python3 --version

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install requests redis boto3 click pytest
```

### Infrastructure Setup

**1. Start Redis:**
```bash
redis-server
# Or via Docker:
docker run -d -p 6379:6379 redis
```

**2. Start LocalStack (for local testing):**
```bash
docker run -d -p 4566:4566 localstack/localstack
```

**3. Configure AWS CLI:**
```bash
# Add to ~/.aws/config
[profile localstack]
endpoint_url = http://localhost:4566
```

**4. Start Kafka (optional):**
```bash
docker-compose up -d kafka
# Or use existing Kafka broker
```

### Running a Scraper

```bash
# Set environment variables
export S3_BUCKET=scraper-testing
export AWS_PROFILE=localstack
export REDIS_HOST=localhost
export REDIS_PORT=6379

# Run MISO wind forecast scraper
python3 sourcing/scraping/miso/wind_forecast/scraper_miso_wind_forecast.py \
    --s3-bucket scraper-testing \
    --aws-profile localstack \
    --environment dev
```

## Development

### Adding a New Scraper

1. **Create directory structure:**
   ```bash
   mkdir -p sourcing/scraping/{provider}/{datasource}/tests/fixtures
   ```

2. **Implement scraper:**
   - Extend `BaseCollector`
   - Implement `generate_candidates()`
   - Implement `collect_content()`
   - Optionally override `validate_content()`

3. **Add version tracking:**
   ```python
   # INFRASTRUCTURE_VERSION: 1.3.0
   # LAST_UPDATED: YYYY-MM-DD
   ```

4. **Create tests:**
   - Unit tests with mocked HTTP/S3/Redis
   - Test fixtures for sample data
   - Integration test documentation

5. **Document:**
   - README.md (usage, API reference)
   - README_INTEGRATION_TEST.md (test results)

### Version Tracking

All scrapers include version tracking headers:
```python
# INFRASTRUCTURE_VERSION: 1.3.0  # Collection framework version
# LAST_UPDATED: 2025-12-02       # Last modification date
```

**Current Infrastructure Version:** 1.3.0

**Features:**
- BaseCollector with S3/Redis/Kafka support
- Gzip compression
- Date partitioning
- Hash deduplication
- Error handling

## License

Internal use only.

## Contact

For questions or issues, contact the data engineering team.
