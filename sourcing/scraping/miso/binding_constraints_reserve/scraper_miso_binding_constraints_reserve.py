"""MISO Binding Constraints Reserve Scraper.

Collects reserve-related binding constraints data from MISO's public API.
API: https://public-api.misoenergy.org/api/BindingConstraints/Reserve

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
    - Reserve-specific constraint validation
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


class MisoBindingConstraintsReserveCollector(BaseCollector):
    """Collector for MISO reserve-related binding constraints data."""

    API_URL = "https://public-api.misoenergy.org/api/BindingConstraints/Reserve"
    TIMEOUT_SECONDS = 30

    # Expected reserve types based on spec
    VALID_RESERVE_TYPES = {"SPIN", "NSPIN", "REG_UP", "REG_DOWN", "SUPPL"}
    VALID_DIRECTIONS = {"UP", "DOWN", "BOTH"}

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate single candidate for current reserve constraints snapshot.

        MISO API only provides current reserve constraints, so we generate one candidate per run.
        """
        collection_time = datetime.now(UTC)
        identifier = f"binding_constraints_reserve_{collection_time.strftime('%Y%m%d_%H%M')}.json"

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=self.API_URL,
            metadata={
                "data_type": "binding_constraints_reserve",
                "source": "miso",
                "collection_timestamp": collection_time.isoformat(),
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-BindingConstraintsReserve-Collector/1.0",
                },
                "timeout": self.TIMEOUT_SECONDS,
            },
            file_date=collection_time.date(),
        )

        logger.info(f"Generated candidate: {identifier}")
        return [candidate]

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch reserve constraints from MISO API."""
        logger.info(f"Fetching reserve constraints from {candidate.source_location}")

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
            raise ScrapingError(f"Failed to fetch reserve constraints: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of reserve constraints response.

        Expected response structure:
        {
          "RefId": "05-Dec-2025, 09:35 EST",
          "Constraint": [
            {
              "Name": "string",
              "Period": "timestamp",
              "Price": number,
              "ReserveType": "string",
              "Direction": "string",
              "Quantity": number,
              "OVERRIDE": boolean,
              "BP1": number,
              "PC1": number,
              "BP2": number,
              "PC2": number
            }
          ]
        }
        """
        try:
            data = json.loads(content)

            # Response should be a dictionary
            if not isinstance(data, dict):
                logger.warning(f"Response is not a dictionary, got {type(data)}")
                return False

            # Check for required top-level fields
            if "RefId" not in data:
                logger.warning("Response missing 'RefId' field")
                return False

            if "Constraint" not in data:
                logger.warning("Response missing 'Constraint' field")
                return False

            # Constraint should be a list
            constraints = data["Constraint"]
            if not isinstance(constraints, list):
                logger.warning(f"Constraint field is not a list, got {type(constraints)}")
                return False

            # RefId should be a non-empty string
            if not isinstance(data["RefId"], str) or not data["RefId"]:
                logger.warning(f"Invalid RefId: {data['RefId']}")
                return False

            # Empty constraint list is valid (no active reserve constraints)
            if len(constraints) == 0:
                logger.info("Valid response with empty Constraint array (no active reserve constraints)")
                return True

            # Validate each constraint entry
            required_fields = [
                "Name", "Period", "Price", "ReserveType",
                "Direction", "Quantity", "OVERRIDE",
                "BP1", "PC1", "BP2", "PC2"
            ]

            for idx, constraint in enumerate(constraints):
                # Check all required fields are present
                for field in required_fields:
                    if field not in constraint:
                        logger.warning(f"Constraint[{idx}] missing required field: {field}")
                        return False

                # Validate Name is non-empty string
                if not isinstance(constraint["Name"], str) or not constraint["Name"]:
                    logger.warning(f"Constraint[{idx}] has invalid Name: {constraint['Name']}")
                    return False

                # Validate Period is non-empty string
                if not isinstance(constraint["Period"], str) or not constraint["Period"]:
                    logger.warning(f"Constraint[{idx}] has invalid Period: {constraint['Period']}")
                    return False

                # Validate Price is numeric
                try:
                    float(constraint["Price"])
                except (ValueError, TypeError):
                    logger.warning(f"Constraint[{idx}] has invalid Price: {constraint['Price']}")
                    return False

                # Validate ReserveType is valid
                reserve_type = constraint["ReserveType"]
                if reserve_type and reserve_type not in self.VALID_RESERVE_TYPES:
                    logger.warning(
                        f"Constraint[{idx}] has unexpected ReserveType: {reserve_type}. "
                        f"Expected one of {self.VALID_RESERVE_TYPES}"
                    )
                    # Log as warning but don't fail - API may add new types

                # Validate Direction is valid
                direction = constraint["Direction"]
                if direction and direction not in self.VALID_DIRECTIONS:
                    logger.warning(
                        f"Constraint[{idx}] has unexpected Direction: {direction}. "
                        f"Expected one of {self.VALID_DIRECTIONS}"
                    )
                    # Log as warning but don't fail

                # Validate Quantity is numeric
                try:
                    float(constraint["Quantity"])
                except (ValueError, TypeError):
                    logger.warning(f"Constraint[{idx}] has invalid Quantity: {constraint['Quantity']}")
                    return False

                # Validate OVERRIDE is boolean or int (0/1)
                override_val = constraint["OVERRIDE"]
                if override_val is not None and not isinstance(override_val, (bool, int)):
                    logger.warning(f"Constraint[{idx}] has invalid OVERRIDE: {override_val}")
                    return False

                # Validate breakpoint parameters are numeric (can be null)
                for bp_field in ["BP1", "PC1", "BP2", "PC2"]:
                    bp_val = constraint[bp_field]
                    if bp_val is not None:
                        try:
                            float(bp_val)
                        except (ValueError, TypeError):
                            logger.warning(f"Constraint[{idx}] has invalid {bp_field}: {bp_val}")
                            return False

            logger.info(
                f"Content validation passed: RefId={data['RefId']}, "
                f"{len(constraints)} reserve constraint(s)"
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
    help="S3 bucket name for storing reserve constraints data",
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
    """Collect MISO reserve-related binding constraints data.

    Example:
        python scraper_miso_binding_constraints_reserve.py --s3-bucket scraper-testing --aws-profile localstack
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO reserve binding constraints collection")
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
    collector = MisoBindingConstraintsReserveCollector(
        dgroup="miso_binding_constraints_reserve",
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
