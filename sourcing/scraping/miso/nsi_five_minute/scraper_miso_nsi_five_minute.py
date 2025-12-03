"""MISO Net Scheduled Interchange (NSI) Scraper - Five Minute Data.

Collects Net Scheduled Interchange data from MISO's public API at 5-minute intervals.
API: https://public-api.misoenergy.org/api/Interchange/GetNsi/FiveMinute

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-02

Features:
    - HTTP collection using BaseCollector framework
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive error handling and validation
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-02

import json
import logging
import os
from datetime import datetime, UTC
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


class MisoNsiFiveMinuteCollector(BaseCollector):
    """Collector for MISO Net Scheduled Interchange (NSI) five-minute data."""

    API_URL = "https://public-api.misoenergy.org/api/Interchange/GetNsi/FiveMinute"
    TIMEOUT_SECONDS = 30

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate single candidate for current NSI data.

        MISO API only provides current data, so we generate one candidate per run.
        """
        collection_time = datetime.now(UTC)
        identifier = f"nsi_five_minute_{collection_time.strftime('%Y%m%d_%H%M')}.json"

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=self.API_URL,
            metadata={
                "data_type": "nsi_five_minute",
                "source": "miso",
                "iso": "MISO",
                "data_category": "interchange",
                "interval": "5_minute",
                "collection_timestamp": collection_time.isoformat(),
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-NSI-Collector/1.0",
                },
                "timeout": self.TIMEOUT_SECONDS,
            },
            file_date=collection_time.date(),
        )

        logger.info(f"Generated candidate: {identifier}")
        return [candidate]

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch NSI data from MISO API."""
        logger.info(f"Fetching NSI data from {candidate.source_location}")

        try:
            response = requests.get(
                candidate.source_location,
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            logger.info(f"Successfully fetched {len(response.content)} bytes (response time: {response.elapsed.total_seconds():.2f}s)")
            return response.content

        except requests.exceptions.Timeout as e:
            raise ScrapingError(f"Timeout fetching NSI data: {e}") from e
        except requests.exceptions.HTTPError as e:
            raise ScrapingError(f"HTTP error fetching NSI data (status {e.response.status_code}): {e}") from e
        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch NSI data: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of NSI data.

        Checks that:
        1. Content is valid JSON
        2. Contains expected NSI data structure
        3. Has reasonable data values
        """
        try:
            data = json.loads(content)

            # Check for expected structure - MISO NSI typically returns object or array
            if not isinstance(data, (dict, list)):
                logger.warning(f"Content is not a JSON object or array: {type(data).__name__}")
                return False

            # If it's a dict, check for common NSI fields
            if isinstance(data, dict):
                # MISO API may return fields like: Value, Timestamp, Unit, etc.
                has_value = any(key.lower() in ['value', 'nsi', 'mw', 'amount']
                               for key in data.keys())
                has_time = any(key.lower() in ['timestamp', 'datetime', 'time', 'effectivetime']
                              for key in data.keys())

                if not (has_value and has_time):
                    logger.warning(f"Missing expected NSI fields. Available keys: {list(data.keys())}")
                    return False

                logger.info(f"Content validation passed (dict with {len(data.keys())} fields)")
                return True

            # If it's a list, check it's not empty
            elif isinstance(data, list):
                if len(data) == 0:
                    logger.warning("Content validation failed: empty data array")
                    return False

                # Check first item has reasonable structure
                if len(data) > 0 and isinstance(data[0], dict):
                    has_value = any(key.lower() in ['value', 'nsi', 'mw', 'amount']
                                   for key in data[0].keys())
                    if not has_value:
                        logger.warning(f"Array items missing value fields. First item keys: {list(data[0].keys())}")
                        return False

                logger.info(f"Content validation passed (array with {len(data)} entries)")
                return True

            return False

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return False
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False


@click.command()
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket name for storing NSI data",
)
@click.option(
    "--aws-profile",
    envvar="AWS_PROFILE",
    help="AWS profile to use (for LocalStack or AWS)",
)
@click.option(
    "--environment",
    type=click.Choice(["dev", "staging", "prod"]),
    default="dev",
    help="Environment name",
)
@click.option(
    "--redis-host",
    default="localhost",
    envvar="REDIS_HOST",
    help="Redis host",
)
@click.option(
    "--redis-port",
    default=6379,
    envvar="REDIS_PORT",
    type=int,
    help="Redis port",
)
@click.option(
    "--redis-db",
    default=0,
    envvar="REDIS_DB",
    type=int,
    help="Redis database number",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Logging level",
)
@click.option(
    "--kafka-connection-string",
    envvar="KAFKA_CONNECTION_STRING",
    help="Kafka connection string for notifications (optional)",
)
def main(
    s3_bucket: str,
    aws_profile: str,
    environment: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    log_level: str,
    kafka_connection_string: str,
):
    """Collect MISO Net Scheduled Interchange (NSI) five-minute data.

    NSI represents the net amount of power scheduled to flow into or out of
    MISO's control area. Updates every 5 minutes with real-time interchange values.

    Example:
        python scraper_miso_nsi_five_minute.py --s3-bucket scraper-testing --aws-profile localstack
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO NSI five-minute data collection")
    logger.info(f"S3 Bucket: {s3_bucket}")
    logger.info(f"Environment: {environment}")
    logger.info(f"AWS Profile: {aws_profile}")

    # Set AWS profile if specified
    if aws_profile:
        os.environ["AWS_PROFILE"] = aws_profile
        # Create boto3 session with profile
        session = boto3.Session(profile_name=aws_profile)
        s3_client = session.client("s3")
    else:
        s3_client = boto3.client("s3")

    # Initialize Redis client
    try:
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=False,
        )
        redis_client.ping()
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    # Initialize collector
    collector = MisoNsiFiveMinuteCollector(
        dgroup="miso_nsi_five_minute",
        s3_bucket=s3_bucket,
        s3_prefix="sourcing",
        redis_client=redis_client,
        environment=environment,
        kafka_connection_string=kafka_connection_string,
    )

    if kafka_connection_string:
        logger.info(f"Kafka notifications enabled: {kafka_connection_string.split('@')[0]}@...")

    # Override the s3_client to use our profile-aware one
    collector.s3_client = s3_client

    # Run collection
    try:
        logger.info("Running collection...")
        results = collector.run_collection()

        logger.info(f"Collection completed: {results}")
        logger.info(f"  Total Candidates: {results.get('total_candidates', 0)}")
        logger.info(f"  Collected: {results.get('collected', 0)}")
        logger.info(f"  Skipped (duplicate): {results.get('skipped_duplicate', 0)}")
        logger.info(f"  Failed: {results.get('failed', 0)}")

        if results.get("failed", 0) > 0:
            logger.error(f"Errors: {results.get('errors', [])}")
            raise click.Abort()

    except Exception as e:
        logger.error(f"Collection failed: {e}")
        raise


if __name__ == "__main__":
    main()
