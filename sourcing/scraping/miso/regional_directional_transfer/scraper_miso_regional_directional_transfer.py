"""MISO Regional Directional Transfer Scraper.

Collects real-time North-South transmission corridor flow data from MISO's public API.
API: https://public-api.misoenergy.org/api/RegionalDirectionalTransfer

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP collection using BaseCollector framework
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive error handling and validation
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

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


class MisoRegionalDirectionalTransferCollector(BaseCollector):
    """Collector for MISO Regional Directional Transfer data.

    This collector fetches real-time North-South transmission corridor flow data,
    including directional flow limits, raw MW measurements, and unidirectional flow values.
    """

    API_URL = "https://public-api.misoenergy.org/api/RegionalDirectionalTransfer"
    TIMEOUT_SECONDS = 30

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate single candidate for current transmission flow snapshot.

        MISO API only provides current 5-minute interval data, so we generate one candidate per run.
        """
        collection_time = datetime.now(UTC)
        identifier = f"regional_directional_transfer_{collection_time.strftime('%Y%m%d_%H%M')}.json"

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=self.API_URL,
            metadata={
                "data_type": "regional_directional_transfer",
                "source": "miso",
                "collection_timestamp": collection_time.isoformat(),
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-RegionalDirectionalTransfer-Collector/1.0",
                },
                "timeout": self.TIMEOUT_SECONDS,
            },
            file_date=collection_time.date(),
        )

        logger.info(f"Generated candidate: {identifier}")
        return [candidate]

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch regional directional transfer data from MISO API."""
        logger.info(f"Fetching regional directional transfer from {candidate.source_location}")

        try:
            response = requests.get(
                candidate.source_location,
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            logger.info(f"Successfully fetched {len(response.content)} bytes")
            return response.content

        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch regional directional transfer: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of regional directional transfer data.

        Expected response structure:
        {
            "RefId": "05-Dec-2025 - Interval 10:35 EST",
            "Interval": [
                {
                    "instantEST": "2025-12-05 10:35:00 AM",
                    "NORTH_SOUTH_LIMIT": "-3000",
                    "SOUTH_NORTH_LIMIT": "2500",
                    "RAW_MW": "1234.56",
                    "UDSFLOW_MW": "987.65"
                }
            ]
        }
        """
        try:
            data = json.loads(content)

            # Response should be a dictionary
            if not isinstance(data, dict):
                logger.warning(f"Response is not a dict, got {type(data)}")
                return False

            # Check required root fields
            if "RefId" not in data:
                logger.warning("Missing required field: RefId")
                return False

            if "Interval" not in data:
                logger.warning("Missing required field: Interval")
                return False

            # Interval should be an array
            if not isinstance(data["Interval"], list):
                logger.warning(f"Interval is not an array, got {type(data['Interval'])}")
                return False

            # Should have at least one interval
            if len(data["Interval"]) == 0:
                logger.warning("Interval array is empty")
                return False

            # Validate first interval object
            interval = data["Interval"][0]
            required_fields = [
                "instantEST",
                "NORTH_SOUTH_LIMIT",
                "SOUTH_NORTH_LIMIT",
                "RAW_MW",
                "UDSFLOW_MW",
            ]

            for field in required_fields:
                if field not in interval:
                    logger.warning(f"Interval missing required field: {field}")
                    return False

            # Validate numeric fields
            try:
                north_south_limit = float(interval["NORTH_SOUTH_LIMIT"])
                south_north_limit = float(interval["SOUTH_NORTH_LIMIT"])
                # Parse RAW_MW to validate it's numeric (value checked in logging below)
                _raw_mw = float(interval["RAW_MW"])
                udsflow_mw = float(interval["UDSFLOW_MW"])

                # Validate flow limits are within expected ranges
                if not (-3500 <= north_south_limit <= 0):
                    logger.warning(f"NORTH_SOUTH_LIMIT out of expected range: {north_south_limit}")
                    return False

                if not (0 <= south_north_limit <= 3000):
                    logger.warning(f"SOUTH_NORTH_LIMIT out of expected range: {south_north_limit}")
                    return False

                # Validate UDSFLOW_MW is non-negative
                if udsflow_mw < 0:
                    logger.warning(f"UDSFLOW_MW should be non-negative: {udsflow_mw}")
                    return False

            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid numeric value in interval data: {e}")
                return False

            logger.info(
                f"Content validation passed (RefId: {data['RefId']}, "
                f"Timestamp: {interval['instantEST']}, "
                f"RAW_MW: {interval['RAW_MW']}, "
                f"UDSFLOW_MW: {interval['UDSFLOW_MW']})"
            )
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return False


@click.command()
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket name for storing regional directional transfer data",
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
    """Collect MISO Regional Directional Transfer data.

    Example:
        python scraper_miso_regional_directional_transfer.py --s3-bucket scraper-testing --aws-profile localstack
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO Regional Directional Transfer collection")
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
    collector = MisoRegionalDirectionalTransferCollector(
        dgroup="miso_regional_directional_transfer",
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
