"""MISO Real-Time Total Load Scraper.

Collects Real-Time Total Load data from MISO's Public API.
Endpoint: https://public-api.misoenergy.org/api/RealTimeTotalLoad

Data includes three time-series arrays:
- ClearedMW: Hourly cleared capacity values
- MediumTermLoadForecast: Hourly forecast values
- FiveMinTotalLoad: 5-minute actual load values

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP REST API collection using BaseCollector framework
    - No authentication required (public API)
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive error handling and validation
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

import json
import logging
from datetime import datetime, timedelta
from typing import List

import boto3
import click
import redis
import requests

from sourcing.infrastructure.collection_framework import (
    BaseCollector,
    DownloadCandidate,
    ScrapingError,
)

logger = logging.getLogger("sourcing_app")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class MisoRealtimeTotalLoadCollector(BaseCollector):
    """Collector for MISO Real-Time Total Load data."""

    BASE_URL = "https://public-api.misoenergy.org/api/RealTimeTotalLoad"
    TIMEOUT_SECONDS = 30

    def __init__(self, start_date: datetime, end_date: datetime, **kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate candidates for each date in the range.

        MISO publishes real-time total load data continuously with 5-minute updates.
        The API returns a snapshot of current data including:
        - Hourly cleared capacity (ClearedMW)
        - Medium-term hourly forecast (MediumTermLoadForecast)
        - 5-minute actual load values (FiveMinTotalLoad)

        We collect snapshots at regular intervals to capture data updates.
        """
        candidates = []
        current_date = self.start_date

        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            date_compact = current_date.strftime('%Y%m%d')

            # Generate identifier with timestamp for uniqueness
            timestamp = current_date.strftime('%Y%m%d_%H%M%S')
            identifier = f"realtime_total_load_{timestamp}.json"
            url = self.BASE_URL

            candidate = DownloadCandidate(
                identifier=identifier,
                source_location=url,
                metadata={
                    "data_type": "realtime_total_load",
                    "source": "miso",
                    "date": date_str,
                    "date_formatted": date_compact,
                    "timestamp": timestamp,
                    "market_type": "real_time_load",
                },
                collection_params={
                    "headers": {
                        "Accept": "application/json",
                        "User-Agent": "MISO-Realtime-Total-Load-Collector/1.0",
                    },
                    "timeout": self.TIMEOUT_SECONDS,
                },
                file_date=current_date.date(),
            )

            candidates.append(candidate)
            logger.info(f"Generated candidate for timestamp: {timestamp}")

            # For real-time data, increment by 5 minutes to match update frequency
            current_date += timedelta(minutes=5)

        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch JSON data from MISO Public API."""
        logger.info(f"Fetching Real-Time Total Load data from {candidate.source_location}")

        try:
            response = requests.get(
                candidate.source_location,
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            # Parse JSON response
            json_data = response.json()

            # Add collection metadata
            result = {
                "data": json_data,
                "collection_metadata": {
                    "collected_at": datetime.now().isoformat(),
                    "source_url": candidate.source_location,
                    **candidate.metadata
                }
            }

            logger.info(
                f"Successfully collected Real-Time Total Load data. "
                f"RefId: {json_data.get('RefId', 'N/A')}, "
                f"ClearedMW: {len(json_data.get('ClearedMW', []))} records, "
                f"Forecast: {len(json_data.get('MediumTermLoadForecast', []))} records, "
                f"5-Min Load: {len(json_data.get('FiveMinTotalLoad', []))} records"
            )

            return json.dumps(result, indent=2).encode('utf-8')

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning("API endpoint returned 404 - data may not be available")
                raise ScrapingError(f"Data not available: {e}") from e
            elif e.response.status_code == 503:
                logger.error("API service unavailable (503)")
                raise ScrapingError(f"Service unavailable: {e}") from e
            raise ScrapingError(f"HTTP error fetching Real-Time Total Load data: {e}") from e
        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch Real-Time Total Load data: {e}") from e
        except json.JSONDecodeError as e:
            raise ScrapingError(f"Invalid JSON response: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of Real-Time Total Load data.

        Expected format:
        {
            "data": {
                "RefId": "2025-12-05T12:35:00Z",
                "ClearedMW": [
                    {"Hour": "12:00", "Value": 32456}
                ],
                "MediumTermLoadForecast": [
                    {"HourEnding": "14:00", "LoadForecast": 35678}
                ],
                "FiveMinTotalLoad": [
                    {"Time": "12:35:00", "Value": 33456}
                ]
            }
        }
        """
        try:
            text_content = content.decode('utf-8')
            result = json.loads(text_content)

            # Check top-level structure
            if "data" not in result:
                logger.error("Missing 'data' field in response")
                return False

            data = result["data"]

            # Check required top-level fields
            if "RefId" not in data:
                logger.error("Missing 'RefId' field in data")
                return False

            required_arrays = ["ClearedMW", "MediumTermLoadForecast", "FiveMinTotalLoad"]
            for array_name in required_arrays:
                if array_name not in data:
                    logger.error(f"Missing required array: {array_name}")
                    return False

                if not isinstance(data[array_name], list):
                    logger.error(f"{array_name} is not a list")
                    return False

            # Validate ClearedMW structure if not empty
            if data["ClearedMW"]:
                cleared_sample = data["ClearedMW"][0]
                if "Hour" not in cleared_sample or "Value" not in cleared_sample:
                    logger.error("ClearedMW records missing required fields (Hour, Value)")
                    return False
                if not isinstance(cleared_sample["Value"], (int, float)):
                    logger.error(f"ClearedMW Value is not numeric: {cleared_sample['Value']}")
                    return False

            # Validate MediumTermLoadForecast structure if not empty
            if data["MediumTermLoadForecast"]:
                forecast_sample = data["MediumTermLoadForecast"][0]
                if "HourEnding" not in forecast_sample or "LoadForecast" not in forecast_sample:
                    logger.error("MediumTermLoadForecast records missing required fields")
                    return False
                if not isinstance(forecast_sample["LoadForecast"], (int, float)):
                    logger.error(f"LoadForecast is not numeric: {forecast_sample['LoadForecast']}")
                    return False

            # Validate FiveMinTotalLoad structure if not empty
            if data["FiveMinTotalLoad"]:
                load_sample = data["FiveMinTotalLoad"][0]
                if "Time" not in load_sample or "Value" not in load_sample:
                    logger.error("FiveMinTotalLoad records missing required fields (Time, Value)")
                    return False
                if not isinstance(load_sample["Value"], (int, float)):
                    logger.error(f"FiveMinTotalLoad Value is not numeric: {load_sample['Value']}")
                    return False

            logger.info(
                f"Validation successful: "
                f"RefId={data['RefId']}, "
                f"ClearedMW={len(data['ClearedMW'])} records, "
                f"Forecast={len(data['MediumTermLoadForecast'])} records, "
                f"5-Min Load={len(data['FiveMinTotalLoad'])} records"
            )
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON content: {str(e)}")
            return False
        except KeyError as e:
            logger.error(f"Missing required key during validation: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected validation error: {str(e)}")
            return False


@click.command()
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    required=True,
    help="Start date/time for data collection (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    required=True,
    help="End date/time for data collection (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"
)
@click.option(
    "--s3-bucket",
    envvar="S3_BUCKET",
    help="S3 bucket for data storage (or set S3_BUCKET environment variable)"
)
@click.option(
    "--aws-profile",
    envvar="AWS_PROFILE",
    help="AWS profile name (or set AWS_PROFILE environment variable)"
)
@click.option(
    "--redis-host",
    default="localhost",
    envvar="REDIS_HOST",
    help="Redis host for deduplication"
)
@click.option(
    "--redis-port",
    default=6379,
    envvar="REDIS_PORT",
    help="Redis port"
)
@click.option(
    "--redis-db",
    default=0,
    envvar="REDIS_DB",
    help="Redis database number"
)
@click.option(
    "--environment",
    type=click.Choice(["dev", "staging", "prod"]),
    default="dev",
    help="Environment for S3 storage and Redis"
)
@click.option(
    "--force",
    is_flag=True,
    help="Force re-download of existing files"
)
@click.option(
    "--skip-hash-check",
    is_flag=True,
    help="Skip Redis hash-based deduplication"
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Logging level"
)
def main(
    start_date: datetime,
    end_date: datetime,
    s3_bucket: str,
    aws_profile: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    environment: str,
    force: bool,
    skip_hash_check: bool,
    log_level: str
) -> None:
    """Collect MISO Real-Time Total Load data.

    This scraper collects real-time load data from MISO's Public API, including:
    - Hourly cleared capacity (ClearedMW)
    - Medium-term hourly load forecast (MediumTermLoadForecast)
    - 5-minute actual load values (FiveMinTotalLoad)

    Data is updated every 5 minutes and provides a 24-hour rolling window of
    forecasts and current load measurements.

    Examples:

        # Collect a single snapshot
        python scraper_miso_realtime_total_load.py \\
            --start-date 2025-12-05T12:00:00 \\
            --end-date 2025-12-05T12:00:00

        # Collect data for a full day at 5-minute intervals
        python scraper_miso_realtime_total_load.py \\
            --start-date 2025-12-05 \\
            --end-date 2025-12-06

        # Use environment variables for credentials
        export S3_BUCKET=your-bucket
        export REDIS_HOST=localhost
        python scraper_miso_realtime_total_load.py \\
            --start-date 2025-12-05T12:00:00 \\
            --end-date 2025-12-05T14:00:00

        # Force re-download existing data
        python scraper_miso_realtime_total_load.py \\
            --start-date 2025-12-05T12:00:00 \\
            --end-date 2025-12-05T12:30:00 \\
            --force
    """
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(
        "Starting MISO Real-Time Total Load collection",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "environment": environment,
            "force": force,
            "skip_hash_check": skip_hash_check
        }
    )

    # Initialize Redis client
    redis_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        decode_responses=False
    )

    # Test Redis connection
    try:
        redis_client.ping()
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}/{redis_db}")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    # Initialize S3 client if needed
    session_kwargs = {}
    if aws_profile:
        session_kwargs["profile_name"] = aws_profile

    session = boto3.Session(**session_kwargs)
    s3_client = session.client("s3")

    if s3_bucket:
        logger.info(f"Using S3 bucket: {s3_bucket}")
    else:
        logger.warning("No S3 bucket specified - files will be validated but not uploaded")

    # Initialize collector
    collector = MisoRealtimeTotalLoadCollector(
        start_date=start_date,
        end_date=end_date,
        dgroup="miso_realtime_total_load",
        s3_bucket=s3_bucket,
        s3_prefix="sourcing",
        redis_client=redis_client,
        environment=environment,
    )

    # Override the s3_client to use our profile-aware one
    collector.s3_client = s3_client

    try:
        results = collector.run_collection()

        logger.info(
            "Collection complete",
            extra={
                "files_downloaded": results.get("files_downloaded", 0),
                "files_skipped": results.get("files_skipped", 0),
                "files_failed": results.get("files_failed", 0),
                "total_processed": results.get("total_processed", 0)
            }
        )

    except Exception as e:
        logger.error(f"Collection failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
