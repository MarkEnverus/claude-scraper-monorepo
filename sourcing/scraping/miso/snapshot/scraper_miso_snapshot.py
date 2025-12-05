"""MISO Snapshot Scraper.

Collects real-time grid status dashboard data from MISO's public API.
API: https://public-api.misoenergy.org/api/Snapshot

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


class MisoSnapshotCollector(BaseCollector):
    """Collector for MISO grid status snapshot data."""

    API_URL = "https://public-api.misoenergy.org/api/Snapshot"
    TIMEOUT_SECONDS = 30

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate single candidate for current grid snapshot.

        MISO API only provides current grid status snapshot, so we generate one candidate per run.
        """
        collection_time = datetime.now(UTC)
        identifier = f"snapshot_{collection_time.strftime('%Y%m%d_%H%M')}.json"

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=self.API_URL,
            metadata={
                "data_type": "snapshot",
                "source": "miso",
                "collection_timestamp": collection_time.isoformat(),
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-Snapshot-Collector/1.0",
                },
                "timeout": self.TIMEOUT_SECONDS,
            },
            file_date=collection_time.date(),
        )

        logger.info(f"Generated candidate: {identifier}")
        return [candidate]

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch grid snapshot from MISO API."""
        logger.info(f"Fetching snapshot from {candidate.source_location}")

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
            raise ScrapingError(f"Failed to fetch snapshot: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of grid snapshot.

        Expected response is an array of 4 metrics:
        - peak_demand_forecast
        - current_demand
        - marginal_energy_cost
        - scheduled_interchange
        """
        try:
            data = json.loads(content)

            # Response should be an array
            if not isinstance(data, list):
                logger.warning(f"Response is not an array, got {type(data)}")
                return False

            # Should have 4 metrics
            if len(data) != 4:
                logger.warning(f"Expected 4 metrics, got {len(data)}")
                return False

            # Expected metric IDs
            expected_ids = {
                "peak_demand_forecast",
                "current_demand",
                "marginal_energy_cost",
                "scheduled_interchange",
            }

            # Check each metric has required fields
            found_ids = set()
            for metric in data:
                required_fields = ["t", "v", "d", "id"]
                for field in required_fields:
                    if field not in metric:
                        logger.warning(f"Metric missing required field: {field}")
                        return False

                found_ids.add(metric["id"])

                # Validate value is numeric
                try:
                    float(metric["v"])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid numeric value for metric {metric['id']}: {metric['v']}")
                    return False

            # Check all expected IDs are present
            if found_ids != expected_ids:
                logger.warning(f"Metric ID mismatch. Expected: {expected_ids}, Found: {found_ids}")
                return False

            logger.info(f"Content validation passed (4 metrics: {', '.join(sorted(found_ids))})")
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return False


@click.command()
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket name for storing snapshot data",
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
    """Collect MISO grid status snapshot data.

    Example:
        python scraper_miso_snapshot.py --s3-bucket scraper-testing --aws-profile localstack
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO snapshot collection")
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
    collector = MisoSnapshotCollector(
        dgroup="miso_snapshot",
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
