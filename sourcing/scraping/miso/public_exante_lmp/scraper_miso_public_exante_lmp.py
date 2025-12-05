"""MISO Public API Ex-Ante LMP Scraper.

Collects real-time forward-looking Locational Marginal Price snapshots from MISO's Public API.
Endpoint: https://public-api.misoenergy.org/api/MarketPricing/GetExAnteLmp

Ex-Ante LMP data represents forecasted prices for intraday trading across market hubs.
This snapshot-based API provides current forward-looking prices for the next 4 hours,
updated every 5 minutes. Used for real-time price discovery and short-term trading strategies.

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP collection using BaseCollector framework
    - Optional query parameters (startTime, endTime, hub)
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive LMP arithmetic validation
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

import json
import logging
from datetime import datetime, UTC
from typing import List, Optional

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


class MisoPublicExAnteLMPCollector(BaseCollector):
    """Collector for MISO Public API Ex-Ante LMP snapshot data."""

    API_URL = "https://public-api.misoenergy.org/api/MarketPricing/GetExAnteLmp"
    TIMEOUT_SECONDS = 30

    def __init__(
        self,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        hub: Optional[str] = None,
        **kwargs
    ):
        """Initialize collector with optional query parameters.

        Args:
            start_time: Optional ISO-8601 datetime (e.g., "2025-12-05T14:30:00Z")
            end_time: Optional ISO-8601 datetime (max 4-hour window from startTime)
            hub: Optional market hub identifier (e.g., "CINERGY", "MINN", "AECI")
            **kwargs: Additional BaseCollector arguments
        """
        super().__init__(**kwargs)
        self.start_time = start_time
        self.end_time = end_time
        self.hub = hub

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate single candidate for current Ex-Ante LMP snapshot.

        MISO Public API provides current forward-looking LMP snapshot, so we generate
        one candidate per run. Snapshot is updated every 5 minutes.
        """
        collection_time = datetime.now(UTC)
        timestamp_str = collection_time.strftime('%Y%m%d_%H%M')

        # Build identifier based on query parameters
        id_parts = ["public_exante_lmp", timestamp_str]
        if self.hub:
            id_parts.append(f"hub_{self.hub}")
        identifier = f"{'_'.join(id_parts)}.json"

        # Build query parameters
        query_params = {}
        if self.start_time:
            query_params["startTime"] = self.start_time
        if self.end_time:
            query_params["endTime"] = self.end_time
        if self.hub:
            query_params["hub"] = self.hub

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=self.API_URL,
            metadata={
                "data_type": "public_exante_lmp",
                "source": "miso",
                "collection_timestamp": collection_time.isoformat(),
                "query_params": query_params,
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-Public-ExAnte-LMP-Collector/1.0",
                },
                "timeout": self.TIMEOUT_SECONDS,
                "query_params": query_params,
            },
            file_date=collection_time.date(),
        )

        logger.info(f"Generated candidate: {identifier}")
        if query_params:
            logger.info(f"Query parameters: {query_params}")

        return [candidate]

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch Ex-Ante LMP snapshot from MISO Public API."""
        logger.info(f"Fetching Public Ex-Ante LMP from {candidate.source_location}")

        try:
            query_params = candidate.collection_params.get("query_params", {})

            response = requests.get(
                candidate.source_location,
                params=query_params,
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            logger.info(f"Successfully fetched {len(response.content)} bytes")
            return response.content

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                logger.error(f"Bad request - check query parameters: {query_params}")
            elif e.response.status_code == 404:
                logger.warning("No data available - may be outside market hours")
            raise ScrapingError(f"HTTP error fetching Public Ex-Ante LMP: {e}") from e
        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch Public Ex-Ante LMP: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure and LMP arithmetic of Public Ex-Ante LMP data.

        Expected response structure:
        {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": [
                {
                    "name": "CINERGY",
                    "lmp": 25.50,
                    "components": {
                        "energy": 24.80,
                        "congestion": 0.45,
                        "losses": 0.25
                    }
                }
            ]
        }

        Validation requirements:
        - timestamp must be valid ISO 8601
        - updateInterval must be "5m"
        - hubs array must not be empty
        - LMP arithmetic: lmp = energy + congestion + losses (within 0.01 tolerance)
        - All price components must be numeric
        """
        try:
            data = json.loads(content)

            # Check top-level required fields
            required_fields = ["timestamp", "updateInterval", "hubs"]
            for field in required_fields:
                if field not in data:
                    logger.error(f"Missing required field: {field}")
                    return False

            # Validate timestamp format (ISO 8601)
            try:
                datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                logger.error(f"Invalid timestamp format: {data['timestamp']}")
                return False

            # Validate updateInterval
            if data["updateInterval"] != "5m":
                logger.warning(f"Unexpected updateInterval: {data['updateInterval']} (expected: 5m)")
                # This is a warning, not a hard validation failure

            # Validate hubs array
            if not isinstance(data["hubs"], list):
                logger.error(f"'hubs' is not an array, got {type(data['hubs'])}")
                return False

            if len(data["hubs"]) == 0:
                logger.warning("Empty hubs array - no data available")
                # Empty is technically valid (no data available)
                return True

            # Validate each hub
            for i, hub in enumerate(data["hubs"]):
                # Check hub required fields
                hub_required = ["name", "lmp", "components"]
                for field in hub_required:
                    if field not in hub:
                        logger.error(f"Hub {i}: Missing required field: {field}")
                        return False

                # Validate hub name
                if not isinstance(hub["name"], str) or not hub["name"].strip():
                    logger.error(f"Hub {i}: Invalid name: {hub.get('name')}")
                    return False

                # Validate LMP is numeric
                if not isinstance(hub["lmp"], (int, float)):
                    logger.error(f"Hub {hub['name']}: LMP is not numeric: {hub['lmp']}")
                    return False

                # Validate components structure
                components = hub["components"]
                component_fields = ["energy", "congestion", "losses"]
                for field in component_fields:
                    if field not in components:
                        logger.error(f"Hub {hub['name']}: Missing component: {field}")
                        return False

                    # Validate component is numeric
                    if not isinstance(components[field], (int, float)):
                        logger.error(f"Hub {hub['name']}: {field} is not numeric: {components[field]}")
                        return False

                # Validate LMP arithmetic: lmp = energy + congestion + losses
                calculated_lmp = (
                    components["energy"] +
                    components["congestion"] +
                    components["losses"]
                )

                if abs(calculated_lmp - hub["lmp"]) > 0.01:
                    logger.error(
                        f"Hub {hub['name']}: LMP arithmetic mismatch - "
                        f"LMP={hub['lmp']}, "
                        f"Energy+Congestion+Losses={calculated_lmp:.2f} "
                        f"(diff={abs(calculated_lmp - hub['lmp']):.4f})"
                    )
                    return False

            logger.info(f"Content validation passed ({len(data['hubs'])} hubs)")
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return False
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Validation error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected validation error: {e}")
            return False


@click.command()
@click.option(
    "--start-time",
    help="Optional start time in ISO-8601 format (e.g., '2025-12-05T14:30:00Z')",
)
@click.option(
    "--end-time",
    help="Optional end time in ISO-8601 format (max 4-hour window from start)",
)
@click.option(
    "--hub",
    help="Optional market hub identifier (e.g., 'CINERGY', 'MINN', 'AECI')",
)
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket name for storing LMP data",
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
    start_time: Optional[str],
    end_time: Optional[str],
    hub: Optional[str],
    s3_bucket: str,
    aws_profile: Optional[str],
    environment: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    log_level: str,
    kafka_connection_string: Optional[str],
):
    """Collect MISO Public API Ex-Ante LMP snapshot data.

    This scraper collects real-time forward-looking Locational Marginal Price snapshots
    from MISO's Public API. The data is updated every 5 minutes and includes LMP values
    and their components (energy, congestion, losses) for market hubs.

    Use case: Real-time price discovery for intraday trading, short-term price forecasting,
    and market monitoring.

    Examples:

        # Collect current snapshot for all hubs
        python scraper_miso_public_exante_lmp.py --s3-bucket my-bucket

        # Collect with time window
        python scraper_miso_public_exante_lmp.py \\
            --s3-bucket my-bucket \\
            --start-time "2025-12-05T14:00:00Z" \\
            --end-time "2025-12-05T18:00:00Z"

        # Collect for specific hub
        python scraper_miso_public_exante_lmp.py \\
            --s3-bucket my-bucket \\
            --hub CINERGY

        # Use with LocalStack for testing
        python scraper_miso_public_exante_lmp.py \\
            --s3-bucket scraper-testing \\
            --aws-profile localstack \\
            --log-level DEBUG
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO Public Ex-Ante LMP collection")
    logger.info(f"S3 Bucket: {s3_bucket}")
    logger.info(f"Environment: {environment}")
    if aws_profile:
        logger.info(f"AWS Profile: {aws_profile}")
    if start_time:
        logger.info(f"Start Time: {start_time}")
    if end_time:
        logger.info(f"End Time: {end_time}")
    if hub:
        logger.info(f"Hub Filter: {hub}")

    # Set up AWS session
    if aws_profile:
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
    collector = MisoPublicExAnteLMPCollector(
        start_time=start_time,
        end_time=end_time,
        hub=hub,
        dgroup="miso_public_exante_lmp",
        s3_bucket=s3_bucket,
        s3_prefix="sourcing",
        redis_client=redis_client,
        environment=environment,
        kafka_connection_string=kafka_connection_string,
    )

    if kafka_connection_string:
        logger.info("Kafka notifications enabled")

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
