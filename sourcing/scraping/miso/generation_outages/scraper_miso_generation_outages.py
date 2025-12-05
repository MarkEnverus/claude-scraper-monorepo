"""MISO Generation Outages Scraper.

Collects generation outage data from MISO's public API.
API: https://public-api.misoenergy.org/api/GenerationOutages/GetGenerationOutagesPlusMinusFiveDays

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


class MisoGenerationOutagesCollector(BaseCollector):
    """Collector for MISO generation outages data."""

    API_URL = "https://public-api.misoenergy.org/api/GenerationOutages/GetGenerationOutagesPlusMinusFiveDays"
    TIMEOUT_SECONDS = 30

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate single candidate for current generation outages.

        MISO API provides a 10-day rolling window (±5 days from current date),
        so we generate one candidate per run.
        """
        collection_time = datetime.now(UTC)
        identifier = f"generation_outages_{collection_time.strftime('%Y%m%d_%H%M')}.json"

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=self.API_URL,
            metadata={
                "data_type": "generation_outages",
                "source": "miso",
                "collection_timestamp": collection_time.isoformat(),
                "window": "plus_minus_five_days",
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-Generation-Outages-Collector/1.0",
                },
                "timeout": self.TIMEOUT_SECONDS,
            },
            file_date=collection_time.date(),
        )

        logger.info(f"Generated candidate: {identifier}")
        return [candidate]

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch generation outages from MISO API."""
        logger.info(f"Fetching generation outages from {candidate.source_location}")

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
            raise ScrapingError(f"Failed to fetch generation outages: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of generation outages."""
        try:
            data = json.loads(content)

            # Check for required top-level fields
            required_fields = ["RefId", "Days"]
            for field in required_fields:
                if field not in data:
                    logger.warning(f"Missing required field: {field}")
                    return False

            # Validate RefId format (should contain "Total Outage Megawatts")
            ref_id = data.get("RefId", "")
            if "Total Outage Megawatts" not in ref_id:
                logger.warning(f"Invalid RefId format: {ref_id}")
                return False

            # Check Days array
            days = data.get("Days", [])
            if not isinstance(days, list):
                logger.warning("'Days' is not a list")
                return False

            if len(days) == 0:
                logger.warning("Empty Days array")
                return False

            # Validate first day entry has required fields
            first_day = days[0]
            required_day_fields = ["OutageDate", "Unplanned", "Planned", "Forced", "Derated"]
            for field in required_day_fields:
                if field not in first_day:
                    logger.warning(f"Missing required field in day entry: {field}")
                    return False

            # Validate numeric fields are non-negative
            numeric_fields = ["Unplanned", "Planned", "Forced", "Derated"]
            for field in numeric_fields:
                value = first_day.get(field)
                if not isinstance(value, (int, float)) or value < 0:
                    logger.warning(f"Invalid value for {field}: {value}")
                    return False

            logger.info(
                f"Content validation passed ({len(days)} days, RefId: {ref_id})"
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
    help="S3 bucket name for storing generation outages data",
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
    """Collect MISO generation outages data.

    Example:
        python scraper_miso_generation_outages.py --s3-bucket scraper-testing --aws-profile localstack
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO generation outages collection")
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
    collector = MisoGenerationOutagesCollector(
        dgroup="miso_generation_outages",
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
