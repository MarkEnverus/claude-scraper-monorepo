"""MISO Net Actual Interchange (NAI) Scraper.

Collects real-time net actual interchange data from MISO's public API.
API: https://public-api.misoenergy.org/api/Interchange/GetNai

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


class MisoNaiCollector(BaseCollector):
    """Collector for MISO Net Actual Interchange data."""

    API_URL = "https://public-api.misoenergy.org/api/Interchange/GetNai"
    TIMEOUT_SECONDS = 30

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate single candidate for current NAI snapshot.

        MISO NAI API provides real-time interchange data, updated every 5 minutes.
        We generate one candidate per run to capture the current state.
        """
        collection_time = datetime.now(UTC)
        identifier = f"nai_{collection_time.strftime('%Y%m%d_%H%M')}.json"

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=self.API_URL,
            metadata={
                "data_type": "net_actual_interchange",
                "source": "miso",
                "collection_timestamp": collection_time.isoformat(),
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-NAI-Collector/1.0",
                },
                "timeout": self.TIMEOUT_SECONDS,
            },
            file_date=collection_time.date(),
        )

        logger.info(f"Generated candidate: {identifier}")
        return [candidate]

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch NAI data from MISO API."""
        logger.info(f"Fetching NAI data from {candidate.source_location}")

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
            raise ScrapingError(f"Failed to fetch NAI data: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of NAI data.

        Expected response structure:
        {
            "data": [
                {
                    "RefId": "MISO_SPP_1",
                    "TieFlowName": "MISO - Southwest Power Pool",
                    "TieFlowValue": 1234.56,
                    "Timestamp": "2025-12-05T14:30:00Z"
                },
                ...
            ]
        }
        """
        try:
            data = json.loads(content)

            # Response should be a dictionary
            if not isinstance(data, dict):
                logger.warning(f"Response is not a dict, got {type(data)}")
                return False

            # Should have 'data' key
            if "data" not in data:
                logger.warning("Response missing 'data' key")
                return False

            # 'data' should be a list
            if not isinstance(data["data"], list):
                logger.warning(f"'data' is not a list, got {type(data['data'])}")
                return False

            # Should have at least 1 tie flow
            if len(data["data"]) == 0:
                logger.warning("No tie flows found in response")
                return False

            # Check each tie flow has required fields and valid data
            required_fields = ["RefId", "TieFlowName", "TieFlowValue", "Timestamp"]
            for idx, tie_flow in enumerate(data["data"]):
                # Check all required fields present
                for field in required_fields:
                    if field not in tie_flow:
                        logger.warning(f"Tie flow {idx} missing required field: {field}")
                        return False

                # Validate TieFlowValue is numeric
                try:
                    float(tie_flow["TieFlowValue"])
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid numeric value for TieFlowValue at index {idx}: {tie_flow['TieFlowValue']}"
                    )
                    return False

                # Validate TieFlowName is non-empty string
                if not isinstance(tie_flow["TieFlowName"], str) or not tie_flow["TieFlowName"].strip():
                    logger.warning(f"Invalid TieFlowName at index {idx}: {tie_flow.get('TieFlowName')}")
                    return False

                # Validate Timestamp is valid ISO 8601
                try:
                    datetime.fromisoformat(tie_flow["Timestamp"].replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    logger.warning(f"Invalid timestamp at index {idx}: {tie_flow.get('Timestamp')}")
                    return False

            logger.info(f"Content validation passed ({len(data['data'])} tie flows)")
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return False


@click.command()
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket name for storing NAI data",
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
    """Collect MISO Net Actual Interchange (NAI) data.

    Example:
        python scraper_miso_nai.py --s3-bucket scraper-testing --aws-profile localstack
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO NAI collection")
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
    collector = MisoNaiCollector(
        dgroup="miso_net_actual_interchange",
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
