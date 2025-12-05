"""MISO Real-Time RSG Commitments Scraper.

Collects real-time Resource-Specific Generation (RSG) commitment data from MISO's public API.
API: https://public-api.misoenergy.org/api/RealTimeRSGCommitments

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
    - Real-time snapshot-based collection (every 5 minutes)
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

import json
import logging
import os
from datetime import datetime, UTC
from typing import List, Dict, Any, Optional

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


class MisoRSGCommitmentsCollector(BaseCollector):
    """Collector for MISO Real-Time RSG Commitments data."""

    API_URL = "https://public-api.misoenergy.org/api/RealTimeRSGCommitments"
    TIMEOUT_SECONDS = 30

    # Valid resource types
    VALID_RESOURCE_TYPES = {
        "GENERATOR",
        "COMBINED_CYCLE",
        "WIND",
        "SOLAR",
        "ENERGY_STORAGE",
        "DEMAND_RESPONSE",
    }

    # Valid commitment reasons
    VALID_COMMITMENT_REASONS = {
        "MUST_RUN",
        "RELIABILITY_MUST_RUN",
        "OUT_OF_MERIT_ECONOMIC",
        "OUT_OF_MERIT_RELIABILITY",
        "CONGESTION_RELIEF",
        "LOCAL_VOLTAGE_SUPPORT",
    }

    def __init__(
        self,
        dgroup: str,
        s3_bucket: str,
        s3_prefix: str,
        redis_client,
        environment: str,
        kafka_connection_string: Optional[str] = None,
        hash_ttl_days: int = 365,
        resource_type: Optional[str] = None,
        commitment_reason: Optional[str] = None,
    ):
        """Initialize RSG Commitments collector.

        Args:
            dgroup: Data group identifier
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix (typically 'sourcing')
            redis_client: Redis client instance
            environment: Environment (dev/staging/prod)
            kafka_connection_string: Optional Kafka connection string
            hash_ttl_days: Hash registry TTL in days (default 365)
            resource_type: Optional filter by resource type
            commitment_reason: Optional filter by commitment reason
        """
        super().__init__(
            dgroup=dgroup,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            redis_client=redis_client,
            environment=environment,
            kafka_connection_string=kafka_connection_string,
            hash_ttl_days=hash_ttl_days,
        )
        self.resource_type = resource_type
        self.commitment_reason = commitment_reason

        # Validate filters if provided
        if resource_type and resource_type not in self.VALID_RESOURCE_TYPES:
            raise ValueError(f"Invalid resource_type: {resource_type}. Must be one of {self.VALID_RESOURCE_TYPES}")
        if commitment_reason and commitment_reason not in self.VALID_COMMITMENT_REASONS:
            raise ValueError(f"Invalid commitment_reason: {commitment_reason}. Must be one of {self.VALID_COMMITMENT_REASONS}")

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate single candidate for current RSG commitments snapshot.

        MISO API provides current real-time commitments, so we generate one candidate per run.
        Optional filters can be applied via query parameters.
        """
        collection_time = datetime.now(UTC)

        # Build query parameters
        query_params: Dict[str, str] = {}
        if self.resource_type:
            query_params["resourceType"] = self.resource_type
        if self.commitment_reason:
            query_params["commitmentReason"] = self.commitment_reason

        # Build identifier with filters
        filter_suffix = ""
        if self.resource_type:
            filter_suffix += f"_type_{self.resource_type.lower()}"
        if self.commitment_reason:
            filter_suffix += f"_reason_{self.commitment_reason.lower()}"

        identifier = f"rsg_commitments{filter_suffix}_{collection_time.strftime('%Y%m%d_%H%M')}.json"

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=self.API_URL,
            metadata={
                "data_type": "rsg_commitments",
                "source": "miso",
                "collection_timestamp": collection_time.isoformat(),
                "resource_type_filter": self.resource_type or "all",
                "commitment_reason_filter": self.commitment_reason or "all",
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-RSG-Commitments-Collector/1.0",
                },
                "query_params": query_params,
                "timeout": self.TIMEOUT_SECONDS,
            },
            file_date=collection_time.date(),
        )

        logger.info(f"Generated candidate: {identifier}")
        return [candidate]

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch RSG commitments from MISO API."""
        logger.info(f"Fetching RSG commitments from {candidate.source_location}")

        try:
            response = requests.get(
                candidate.source_location,
                headers=candidate.collection_params.get("headers", {}),
                params=candidate.collection_params.get("query_params", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            logger.info(f"Successfully fetched {len(response.content)} bytes")
            return response.content

        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch RSG commitments: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure and data quality of RSG commitments.

        Expected response structure:
        {
            "data": [
                {
                    "resourceId": "ALTW.WELLS1",
                    "resourceName": "Wells 1",
                    "resourceType": "GENERATOR",
                    "commitmentStart": "2025-12-05T10:00:00-05:00",
                    "commitmentEnd": "2025-12-05T12:00:00-05:00",
                    "commitmentReason": "MUST_RUN",
                    "economicMaximum": 100.0,
                    "economicMinimum": 20.0,
                    "actualOutput": 50.0,
                    "marginalCost": 25.5,
                    "startupCost": 1000.0,
                    "minimumRunTime": 60
                },
                ...
            ],
            "metadata": {
                "timestamp": "2025-12-05T10:05:00-05:00",
                "totalCommitments": 150,
                "updateInterval": "PT5M"
            }
        }
        """
        try:
            data = json.loads(content)

            # Response should be a dict with 'data' and 'metadata' keys
            if not isinstance(data, dict):
                logger.warning(f"Response is not a dict, got {type(data)}")
                return False

            if "data" not in data:
                logger.warning("Response missing 'data' key")
                return False

            if "metadata" not in data:
                logger.warning("Response missing 'metadata' key")
                return False

            commitments = data["data"]
            metadata = data["metadata"]

            # Data should be an array
            if not isinstance(commitments, list):
                logger.warning(f"'data' is not a list, got {type(commitments)}")
                return False

            # Metadata validation
            required_metadata_fields = ["timestamp", "totalCommitments", "updateInterval"]
            for field in required_metadata_fields:
                if field not in metadata:
                    logger.warning(f"Metadata missing required field: {field}")
                    return False

            # Validate totalCommitments matches actual count
            if metadata["totalCommitments"] != len(commitments):
                logger.warning(
                    f"Metadata totalCommitments ({metadata['totalCommitments']}) "
                    f"doesn't match actual count ({len(commitments)})"
                )
                return False

            # Validate each commitment record
            for i, commitment in enumerate(commitments):
                if not self._validate_commitment_record(commitment, i):
                    return False

            logger.info(
                f"Content validation passed ({len(commitments)} commitments, "
                f"metadata timestamp: {metadata['timestamp']})"
            )
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return False

    def _validate_commitment_record(self, commitment: Dict[str, Any], index: int) -> bool:
        """Validate a single commitment record.

        Args:
            commitment: Commitment record to validate
            index: Index in the array (for error reporting)

        Returns:
            True if valid, False otherwise
        """
        # Required fields
        required_fields = [
            "resourceId",
            "resourceName",
            "resourceType",
            "commitmentStart",
            "commitmentEnd",
            "commitmentReason",
            "economicMaximum",
            "economicMinimum",
            "actualOutput",
            "marginalCost",
            "startupCost",
            "minimumRunTime",
        ]

        for field in required_fields:
            if field not in commitment:
                logger.warning(f"Commitment {index} missing required field: {field}")
                return False

        # Validate resource type
        if commitment["resourceType"] not in self.VALID_RESOURCE_TYPES:
            logger.warning(
                f"Commitment {index} has invalid resourceType: {commitment['resourceType']}. "
                f"Must be one of {self.VALID_RESOURCE_TYPES}"
            )
            return False

        # Validate commitment reason
        if commitment["commitmentReason"] not in self.VALID_COMMITMENT_REASONS:
            logger.warning(
                f"Commitment {index} has invalid commitmentReason: {commitment['commitmentReason']}. "
                f"Must be one of {self.VALID_COMMITMENT_REASONS}"
            )
            return False

        # Validate numeric fields are numeric
        numeric_fields = {
            "economicMaximum": float,
            "economicMinimum": float,
            "actualOutput": float,
            "marginalCost": float,
            "startupCost": float,
        }

        for field, expected_type in numeric_fields.items():
            try:
                expected_type(commitment[field])
            except (ValueError, TypeError):
                logger.warning(
                    f"Commitment {index} has invalid {field}: {commitment[field]} "
                    f"(expected {expected_type.__name__})"
                )
                return False

        # Validate integer fields are integers
        try:
            minimum_run_time = int(commitment["minimumRunTime"])
        except (ValueError, TypeError):
            logger.warning(
                f"Commitment {index} has invalid minimumRunTime: {commitment['minimumRunTime']} "
                f"(expected int)"
            )
            return False

        # Validate economic constraints
        econ_max = float(commitment["economicMaximum"])
        econ_min = float(commitment["economicMinimum"])
        actual_output = float(commitment["actualOutput"])

        if econ_max < econ_min:
            logger.warning(
                f"Commitment {index} economicMaximum ({econ_max}) < economicMinimum ({econ_min})"
            )
            return False

        # Validate non-negative MW values
        if econ_max < 0:
            logger.warning(f"Commitment {index} economicMaximum ({econ_max}) is negative")
            return False

        if econ_min < 0:
            logger.warning(f"Commitment {index} economicMinimum ({econ_min}) is negative")
            return False

        if actual_output < 0:
            logger.warning(f"Commitment {index} actualOutput ({actual_output}) is negative")
            return False

        # Validate actual output is within reasonable bounds (allow 10% tolerance for ramp rates)
        tolerance = 0.10
        if actual_output < econ_min * (1 - tolerance) or actual_output > econ_max * (1 + tolerance):
            logger.warning(
                f"Commitment {index} actualOutput ({actual_output}) outside reasonable bounds: "
                f"[{econ_min * (1 - tolerance)}, {econ_max * (1 + tolerance)}] "
                f"(economicMinimum={econ_min}, economicMaximum={econ_max})"
            )
            return False

        # Validate minimum run time is positive
        if minimum_run_time <= 0:
            logger.warning(f"Commitment {index} minimumRunTime ({minimum_run_time}) must be positive")
            return False

        # Validate datetime fields
        try:
            start = datetime.fromisoformat(commitment["commitmentStart"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(commitment["commitmentEnd"].replace("Z", "+00:00"))

            if end <= start:
                logger.warning(
                    f"Commitment {index} commitmentEnd ({end}) must be after commitmentStart ({start})"
                )
                return False
        except (ValueError, AttributeError) as e:
            logger.warning(
                f"Commitment {index} has invalid datetime format: {e}"
            )
            return False

        return True


@click.command()
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket name for storing RSG commitments data",
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
    "--resource-type",
    type=click.Choice([
        "GENERATOR",
        "COMBINED_CYCLE",
        "WIND",
        "SOLAR",
        "ENERGY_STORAGE",
        "DEMAND_RESPONSE",
    ]),
    help="Filter by resource type",
)
@click.option(
    "--commitment-reason",
    type=click.Choice([
        "MUST_RUN",
        "RELIABILITY_MUST_RUN",
        "OUT_OF_MERIT_ECONOMIC",
        "OUT_OF_MERIT_RELIABILITY",
        "CONGESTION_RELIEF",
        "LOCAL_VOLTAGE_SUPPORT",
    ]),
    help="Filter by commitment reason",
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
    resource_type: Optional[str],
    commitment_reason: Optional[str],
    log_level: str,
    kafka_connection_string: str,
):
    """Collect MISO Real-Time RSG Commitments data.

    Example:
        python scraper_miso_rsg_commitments.py --s3-bucket scraper-testing --aws-profile localstack

    Example with filters:
        python scraper_miso_rsg_commitments.py --s3-bucket scraper-testing --resource-type WIND
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO RSG Commitments collection")
    logger.info(f"S3 Bucket: {s3_bucket}")
    logger.info(f"Environment: {environment}")
    logger.info(f"AWS Profile: {aws_profile}")
    if resource_type:
        logger.info(f"Resource Type Filter: {resource_type}")
    if commitment_reason:
        logger.info(f"Commitment Reason Filter: {commitment_reason}")

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
    collector = MisoRSGCommitmentsCollector(
        dgroup="miso_rsg_commitments",
        s3_bucket=s3_bucket,
        s3_prefix="sourcing",
        redis_client=redis_client,
        environment=environment,
        kafka_connection_string=kafka_connection_string,
        resource_type=resource_type,
        commitment_reason=commitment_reason,
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
