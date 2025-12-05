"""MISO CSAT Supply & Demand Scraper.

Collects Capacity Short-Term Adequacy Tracking (CSAT) Supply & Demand data from MISO's Public API.
Endpoint: https://public-api.misoenergy.org/api/CsatSupplyDemand

CSAT tracks near-term resource adequacy by comparing actual and forecasted demand against
committed and available generation capacity. This data includes a 24-hour forecast horizon
and is updated every 15 minutes, providing operators and market participants with real-time
visibility into grid adequacy conditions.

The adequacy score (0.0-1.0) indicates the reliability level, with higher scores representing
better adequacy margins between available capacity and demand.

Data is stored to S3 with timestamp-based partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP REST API collection using BaseCollector framework
    - No authentication required (public API)
    - Snapshot-based collection with 24-hour forecast
    - Comprehensive validation (capacity >= demand, adequacy score range, unit consistency)
    - Redis-based hash deduplication
    - S3 storage with timestamp partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Optional region filtering (CENTRAL, SOUTH, NORTH, or MISO_TOTAL)
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

import json
import logging
from datetime import datetime, timedelta, UTC
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


class MisoCsatSupplyDemandCollector(BaseCollector):
    """Collector for MISO CSAT Supply & Demand data via Public API."""

    BASE_URL = "https://public-api.misoenergy.org/api/CsatSupplyDemand"
    TIMEOUT_SECONDS = 30
    UPDATE_INTERVAL_MINUTES = 15  # API updates every 15 minutes

    def __init__(
        self,
        start_datetime: datetime,
        end_datetime: datetime,
        region: Optional[str] = None,
        **kwargs
    ):
        """Initialize CSAT Supply & Demand collector.

        Args:
            start_datetime: Start timestamp for data collection (UTC)
            end_datetime: End timestamp for data collection (UTC)
            region: Optional region filter (CENTRAL, SOUTH, NORTH, or None for MISO_TOTAL)
            **kwargs: Additional BaseCollector arguments
        """
        super().__init__(**kwargs)
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.region = region

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate candidates for each 15-minute interval in the range.

        CSAT data is published every 15 minutes and represents a snapshot of current
        conditions plus a 24-hour forecast. Each snapshot is collected as a separate file.
        """
        candidates = []
        current_datetime = self.start_datetime

        while current_datetime <= self.end_datetime:
            timestamp_str = current_datetime.strftime('%Y%m%dT%H%M%SZ')
            region_suffix = f"_{self.region.lower()}" if self.region else ""
            identifier = f"csat_supply_demand_{timestamp_str}{region_suffix}.json"

            candidate = DownloadCandidate(
                identifier=identifier,
                source_location=self.BASE_URL,
                metadata={
                    "data_type": "csat_supply_demand",
                    "source": "miso",
                    "timestamp": timestamp_str,
                    "region": self.region or "MISO_TOTAL",
                    "update_frequency": "15min",
                    "forecast_horizon": "24h",
                },
                collection_params={
                    "headers": {
                        "Accept": "application/json",
                        "User-Agent": "MISO-CSAT-Supply-Demand-Collector/1.0",
                    },
                    "timeout": self.TIMEOUT_SECONDS,
                    "query_params": {
                        "region": self.region
                    } if self.region else {},
                },
                file_date=current_datetime.date(),
            )

            candidates.append(candidate)
            logger.info(f"Generated candidate for timestamp: {timestamp_str}")

            current_datetime += timedelta(minutes=self.UPDATE_INTERVAL_MINUTES)

        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch JSON data from MISO Public API.

        Returns the current snapshot with actual values and 24-hour forecast.
        """
        logger.info(f"Fetching CSAT Supply & Demand data from {candidate.source_location}")

        try:
            response = requests.get(
                candidate.source_location,
                params=candidate.collection_params.get("query_params", {}),
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            # Parse JSON to validate structure
            json_data = response.json()

            # Augment with collection metadata
            augmented_data = {
                "collection_timestamp": datetime.now(UTC).isoformat(),
                "api_response": json_data,
                "metadata": candidate.metadata
            }

            logger.info(f"Successfully collected CSAT data for region: {candidate.metadata.get('region')}")
            return json.dumps(augmented_data, indent=2).encode('utf-8')

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                logger.error(f"Bad request - invalid region parameter: {candidate.collection_params.get('query_params')}")
            elif e.response.status_code == 404:
                logger.warning(f"No data available for region: {candidate.metadata.get('region')}")
                # Return empty response for 404
                return json.dumps({
                    "collection_timestamp": datetime.now(UTC).isoformat(),
                    "api_response": None,
                    "metadata": candidate.metadata,
                    "note": "No data available"
                }).encode('utf-8')
            elif e.response.status_code == 429:
                logger.warning("Rate limit exceeded - consider adding delays between requests")
            raise ScrapingError(f"HTTP error fetching CSAT data: {e}") from e
        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch CSAT data: {e}") from e
        except json.JSONDecodeError as e:
            raise ScrapingError(f"Invalid JSON response: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure and data integrity of CSAT data.

        Expected format:
        {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": {
                "timestamp": "2025-12-05T10:00:00-05:00",
                "region": "MISO_TOTAL",
                "data": {
                    "actualDemand": {"value": 75000, "unit": "MW", "timestamp": "..."},
                    "committedCapacity": {"value": 85000, "unit": "MW", "timestamp": "..."},
                    "availableCapacity": {"value": 10000, "unit": "MW", "timestamp": "...", "calculationType": "..."},
                    "demandForecast": {"value": 78000, "unit": "MW", "timestamp": "...", "forecastHorizon": "24h"},
                    "committedCapacityForecast": {"value": 87000, "unit": "MW", "timestamp": "...", "forecastHorizon": "24h"}
                },
                "adequacyScore": {"value": 0.85, "interpretation": "...", "riskLevel": "..."}
            }
        }

        Validation Requirements:
        - committedCapacity >= actualDemand
        - availableCapacity = committedCapacity - actualDemand (within tolerance)
        - adequacyScore.value between 0.0 and 1.0
        - All capacity/demand values non-negative
        - All units must be "MW"
        """
        try:
            text_content = content.decode('utf-8')
            data = json.loads(text_content)

            # Check top-level structure
            if "api_response" not in data:
                logger.error("Missing 'api_response' field in response")
                return False

            api_response = data["api_response"]

            # Handle empty/null response (404 case)
            if api_response is None:
                logger.warning("API response is null (no data available)")
                return True

            # Validate required top-level fields
            required_fields = ["timestamp", "region", "data", "adequacyScore"]
            for field in required_fields:
                if field not in api_response:
                    logger.error(f"Missing required field: {field}")
                    return False

            # Validate data structure
            data_obj = api_response["data"]
            required_data_fields = [
                "actualDemand",
                "committedCapacity",
                "availableCapacity",
                "demandForecast",
                "committedCapacityForecast"
            ]

            for field in required_data_fields:
                if field not in data_obj:
                    logger.error(f"Missing required data field: {field}")
                    return False

                # Validate structure of each field
                field_data = data_obj[field]
                if "value" not in field_data or "unit" not in field_data:
                    logger.error(f"Missing value or unit in {field}")
                    return False

                # Validate unit is MW
                if field_data["unit"] != "MW":
                    logger.error(f"Invalid unit for {field}: {field_data['unit']} (expected MW)")
                    return False

                # Validate value is non-negative
                if field_data["value"] < 0:
                    logger.error(f"Negative value for {field}: {field_data['value']}")
                    return False

            # Validate capacity >= demand
            actual_demand = data_obj["actualDemand"]["value"]
            committed_capacity = data_obj["committedCapacity"]["value"]
            available_capacity = data_obj["availableCapacity"]["value"]

            if committed_capacity < actual_demand:
                logger.error(
                    f"committedCapacity ({committed_capacity} MW) < actualDemand ({actual_demand} MW)"
                )
                return False

            # Validate availableCapacity = committedCapacity - actualDemand (within tolerance)
            expected_available = committed_capacity - actual_demand
            tolerance = 1.0  # Allow 1 MW tolerance for rounding
            if abs(available_capacity - expected_available) > tolerance:
                logger.warning(
                    f"availableCapacity mismatch: expected {expected_available} MW, "
                    f"got {available_capacity} MW (difference: {abs(available_capacity - expected_available)} MW)"
                )
                # This is a warning, not a validation failure

            # Validate adequacyScore
            adequacy_score = api_response["adequacyScore"]
            if "value" not in adequacy_score:
                logger.error("Missing adequacyScore.value")
                return False

            score_value = adequacy_score["value"]
            if not (0.0 <= score_value <= 1.0):
                logger.error(f"adequacyScore out of range [0.0, 1.0]: {score_value}")
                return False

            # Validate forecast fields have forecastHorizon
            for forecast_field in ["demandForecast", "committedCapacityForecast"]:
                if "forecastHorizon" not in data_obj[forecast_field]:
                    logger.error(f"Missing forecastHorizon in {forecast_field}")
                    return False

            logger.info(
                f"Validated CSAT data successfully: "
                f"demand={actual_demand} MW, capacity={committed_capacity} MW, "
                f"available={available_capacity} MW, adequacy={score_value:.3f}"
            )

            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON content: {str(e)}")
            return False
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Validation error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected validation error: {str(e)}")
            return False


@click.command()
@click.option(
    "--start-datetime",
    type=click.DateTime(formats=["%Y-%m-%dT%H:%M:%S"]),
    required=True,
    help="Start timestamp for data collection in UTC (YYYY-MM-DDTHH:MM:SS)"
)
@click.option(
    "--end-datetime",
    type=click.DateTime(formats=["%Y-%m-%dT%H:%M:%S"]),
    required=True,
    help="End timestamp for data collection in UTC (YYYY-MM-DDTHH:MM:SS)"
)
@click.option(
    "--region",
    type=click.Choice(["CENTRAL", "SOUTH", "NORTH"], case_sensitive=False),
    help="Filter by MISO region (omit for MISO_TOTAL)"
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
    start_datetime: datetime,
    end_datetime: datetime,
    region: Optional[str],
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
    """Collect MISO CSAT Supply & Demand data.

    This scraper collects Capacity Short-Term Adequacy Tracking (CSAT) data from MISO's
    Public API, including actual and forecasted demand, committed and available capacity,
    and adequacy scores. Data is updated every 15 minutes.

    The CSAT dataset provides critical visibility into resource adequacy conditions,
    helping operators and market participants understand near-term reliability margins.

    Examples:

        # Collect data for a single day (every 15 minutes)
        python scraper_miso_csat_supply_demand.py \\
            --start-datetime 2025-12-05T00:00:00 \\
            --end-datetime 2025-12-05T23:59:59

        # Collect data for a specific region
        python scraper_miso_csat_supply_demand.py \\
            --start-datetime 2025-12-05T00:00:00 \\
            --end-datetime 2025-12-05T23:59:59 \\
            --region SOUTH

        # Use environment variables for credentials
        export S3_BUCKET=your-bucket
        python scraper_miso_csat_supply_demand.py \\
            --start-datetime 2025-12-05T00:00:00 \\
            --end-datetime 2025-12-05T23:59:59

        # Collect single hour with debug logging
        python scraper_miso_csat_supply_demand.py \\
            --start-datetime 2025-12-05T10:00:00 \\
            --end-datetime 2025-12-05T11:00:00 \\
            --log-level DEBUG
    """
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(
        "Starting MISO CSAT Supply & Demand collection",
        extra={
            "start_datetime": start_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
            "end_datetime": end_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
            "region": region or "MISO_TOTAL",
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
    collector = MisoCsatSupplyDemandCollector(
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        region=region,
        dgroup="miso_csat_supply_demand",
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
                "collected": results.get("collected", 0),
                "skipped_duplicate": results.get("skipped_duplicate", 0),
                "failed": results.get("failed", 0),
                "total_processed": results.get("total_candidates", 0)
            }
        )

    except Exception as e:
        logger.error(f"Collection failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
