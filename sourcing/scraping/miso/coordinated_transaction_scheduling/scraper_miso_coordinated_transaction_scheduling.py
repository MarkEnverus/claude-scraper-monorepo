"""MISO Coordinated Transaction Scheduling (CTS) Scraper.

Collects MISO-PJM interface forecasts from MISO's public CTS API.
API: https://public-api.misoenergy.org/api/CoordinatedTransactionScheduling

Data includes:
- 5-minute interval forecasted LMPs for MISO-PJM interface nodes
- LMP components (energy, congestion, losses)
- Transaction volumes and directions between MISO and PJM
- Operating day forecasts with real-time updates every 15 minutes

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP collection using BaseCollector framework
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive validation for LMP arithmetic and data quality
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

import json
import logging
import os
from datetime import datetime, UTC, timedelta, date
from typing import List, Optional, Dict, Any

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


class MisoCtsCollector(BaseCollector):
    """Collector for MISO Coordinated Transaction Scheduling (CTS) data."""

    API_URL = "https://public-api.misoenergy.org/api/CoordinatedTransactionScheduling"
    TIMEOUT_SECONDS = 30
    VALID_INTERVALS = ["00", "05", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55"]
    VALID_LMP_DIRECTIONS = ["export", "import", "neutral"]
    VALID_TRANSACTION_DIRECTIONS = ["MISO_TO_PJM", "PJM_TO_MISO", "BALANCED"]
    VALID_DATA_QUALITIES = ["FORECAST", "PRELIMINARY", "VALIDATED"]

    def generate_candidates(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        date_param: Optional[str] = None,
        interval: Optional[str] = None,
        node: Optional[str] = None,
        **kwargs: Any
    ) -> List[DownloadCandidate]:
        """Generate candidates for CTS API data.

        Args:
            start_date: Start date for date range collection (optional)
            end_date: End date for date range collection (optional)
            date_param: Specific date in YYYY-MM-DD format (optional)
            interval: Specific 5-minute interval in HH:MM format (optional)
            node: Specific MISO-PJM interface node identifier (optional)

        Returns:
            List of DownloadCandidate objects

        Note:
            - If no parameters provided, collects current snapshot
            - If date range provided, generates candidates for each day
            - Query parameters are optional and filter the API response
        """
        candidates: List[DownloadCandidate] = []
        collection_time = datetime.now(UTC)

        # Determine dates to collect
        dates_to_collect: List[Optional[str]] = []

        if date_param:
            # Specific date provided
            dates_to_collect.append(date_param)
        elif start_date and end_date:
            # Date range provided
            current = start_date
            while current <= end_date:
                dates_to_collect.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
        else:
            # No date specified - collect current snapshot
            dates_to_collect.append(None)

        for date_value in dates_to_collect:
            # Build query parameters
            query_params: Dict[str, str] = {}
            if date_value:
                query_params["date"] = date_value
            if interval:
                query_params["interval"] = interval
            if node:
                query_params["node"] = node

            # Build identifier
            identifier_parts = ["cts"]
            if date_value:
                identifier_parts.append(date_value.replace("-", ""))
            else:
                identifier_parts.append(collection_time.strftime("%Y%m%d_%H%M"))
            if interval:
                identifier_parts.append(f"int{interval.replace(':', '')}")
            if node:
                # Sanitize node name for filename
                node_clean = node.replace(".", "_").replace(" ", "_")
                identifier_parts.append(node_clean)

            identifier = "_".join(identifier_parts) + ".json"

            # Determine file_date for S3 partitioning
            if date_value:
                file_date = datetime.strptime(date_value, "%Y-%m-%d").date()
            else:
                file_date = collection_time.date()

            candidate = DownloadCandidate(
                identifier=identifier,
                source_location=self.API_URL,
                metadata={
                    "data_type": "coordinated_transaction_scheduling",
                    "source": "miso",
                    "collection_timestamp": collection_time.isoformat(),
                    "operating_day": date_value or "current",
                    "interval": interval or "all",
                    "node": node or "all",
                },
                collection_params={
                    "query_params": query_params,
                    "headers": {
                        "Accept": "application/json",
                        "User-Agent": "MISO-CTS-Collector/1.0",
                    },
                    "timeout": self.TIMEOUT_SECONDS,
                },
                file_date=file_date,
            )

            candidates.append(candidate)
            logger.info(f"Generated candidate: {identifier}")

        logger.info(f"Total candidates generated: {len(candidates)}")
        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch CTS data from MISO API."""
        query_params = candidate.collection_params.get("query_params", {})
        logger.info(
            f"Fetching CTS data from {candidate.source_location}",
            extra={"query_params": query_params}
        )

        try:
            response = requests.get(
                candidate.source_location,
                params=query_params,
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            logger.info(f"Successfully fetched {len(response.content)} bytes")
            return response.content

        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch CTS data: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate CTS JSON structure and data quality.

        Validation checks:
        - Valid JSON structure
        - Required top-level fields (data, metadata)
        - Data array contains forecast objects
        - LMP arithmetic: total = energy + congestion + losses (within 0.01 tolerance)
        - Interval format: HH:MM with valid minutes
        - Direction values match expected enums
        - Numeric fields are valid numbers
        - Transaction volumes are non-negative
        """
        try:
            data = json.loads(content)

            # Check top-level structure
            if not isinstance(data, dict):
                logger.warning(f"Response is not a dict, got {type(data)}")
                return False

            if "data" not in data or "metadata" not in data:
                logger.warning("Missing required top-level fields: data or metadata")
                return False

            # Validate data array
            forecast_data = data["data"]
            if not isinstance(forecast_data, list):
                logger.warning(f"'data' field is not a list, got {type(forecast_data)}")
                return False

            if len(forecast_data) == 0:
                logger.warning("'data' array is empty")
                return False

            # Validate metadata
            metadata = data["metadata"]
            if not isinstance(metadata, dict):
                logger.warning(f"'metadata' field is not a dict, got {type(metadata)}")
                return False

            required_metadata_fields = ["operatingDay", "retrievalTimestamp", "dataQuality"]
            for field in required_metadata_fields:
                if field not in metadata:
                    logger.warning(f"Metadata missing required field: {field}")
                    return False

            # Validate data quality enum
            if metadata["dataQuality"] not in self.VALID_DATA_QUALITIES:
                logger.warning(
                    f"Invalid dataQuality: {metadata['dataQuality']}, "
                    f"expected one of {self.VALID_DATA_QUALITIES}"
                )
                return False

            # Validate each forecast object
            for idx, forecast in enumerate(forecast_data):
                # Check required fields
                required_fields = ["timestamp", "interval", "node", "forecastedLMP", "transactionVolume"]
                for field in required_fields:
                    if field not in forecast:
                        logger.warning(f"Forecast {idx} missing required field: {field}")
                        return False

                # Validate interval format
                interval_value = forecast["interval"]
                if not self._validate_interval_format(interval_value):
                    logger.warning(f"Forecast {idx} has invalid interval format: {interval_value}")
                    return False

                # Validate forecastedLMP structure
                lmp = forecast["forecastedLMP"]
                if not isinstance(lmp, dict):
                    logger.warning(f"Forecast {idx} forecastedLMP is not a dict")
                    return False

                lmp_required = ["total", "components", "direction"]
                for field in lmp_required:
                    if field not in lmp:
                        logger.warning(f"Forecast {idx} forecastedLMP missing field: {field}")
                        return False

                # Validate LMP direction
                if lmp["direction"] not in self.VALID_LMP_DIRECTIONS:
                    logger.warning(
                        f"Forecast {idx} invalid LMP direction: {lmp['direction']}, "
                        f"expected one of {self.VALID_LMP_DIRECTIONS}"
                    )
                    return False

                # Validate LMP components
                components = lmp["components"]
                if not isinstance(components, dict):
                    logger.warning(f"Forecast {idx} LMP components is not a dict")
                    return False

                component_fields = ["energy", "congestion", "losses"]
                for field in component_fields:
                    if field not in components:
                        logger.warning(f"Forecast {idx} LMP components missing: {field}")
                        return False

                # Validate LMP arithmetic: total = energy + congestion + losses
                try:
                    total = float(lmp["total"])
                    energy = float(components["energy"])
                    congestion = float(components["congestion"])
                    losses = float(components["losses"])

                    calculated_total = energy + congestion + losses
                    if abs(total - calculated_total) > 0.01:
                        logger.warning(
                            f"Forecast {idx} LMP arithmetic mismatch: "
                            f"total={total}, calculated={calculated_total} "
                            f"(energy={energy} + congestion={congestion} + losses={losses})"
                        )
                        return False

                except (ValueError, TypeError) as e:
                    logger.warning(f"Forecast {idx} invalid numeric LMP values: {e}")
                    return False

                # Validate transactionVolume structure
                volume = forecast["transactionVolume"]
                if not isinstance(volume, dict):
                    logger.warning(f"Forecast {idx} transactionVolume is not a dict")
                    return False

                if "mwh" not in volume or "direction" not in volume:
                    logger.warning(f"Forecast {idx} transactionVolume missing mwh or direction")
                    return False

                # Validate transaction direction
                if volume["direction"] not in self.VALID_TRANSACTION_DIRECTIONS:
                    logger.warning(
                        f"Forecast {idx} invalid transaction direction: {volume['direction']}, "
                        f"expected one of {self.VALID_TRANSACTION_DIRECTIONS}"
                    )
                    return False

                # Validate transaction volume is non-negative
                try:
                    mwh = float(volume["mwh"])
                    if mwh < 0:
                        logger.warning(f"Forecast {idx} negative transaction volume: {mwh}")
                        return False
                except (ValueError, TypeError) as e:
                    logger.warning(f"Forecast {idx} invalid transaction volume: {e}")
                    return False

            # Validate operating day matches date parameter if provided
            query_params = candidate.collection_params.get("query_params", {})
            if "date" in query_params:
                expected_date = query_params["date"]
                actual_date = metadata["operatingDay"]
                if expected_date != actual_date:
                    logger.warning(
                        f"Operating day mismatch: expected {expected_date}, got {actual_date}"
                    )
                    return False

            logger.info(
                f"Content validation passed: {len(forecast_data)} forecasts, "
                f"quality={metadata['dataQuality']}, "
                f"operating_day={metadata['operatingDay']}"
            )
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return False
        except Exception as e:
            logger.error(f"Validation error: {e}", exc_info=True)
            return False

    def _validate_interval_format(self, interval: str) -> bool:
        """Validate interval is in HH:MM format with valid minutes.

        Args:
            interval: Interval string (e.g., "00:00", "14:35")

        Returns:
            True if valid, False otherwise
        """
        if not isinstance(interval, str):
            return False

        parts = interval.split(":")
        if len(parts) != 2:
            return False

        hour, minute = parts

        # Validate hour (00-23)
        try:
            hour_int = int(hour)
            if hour_int < 0 or hour_int > 23:
                return False
        except ValueError:
            return False

        # Validate minute is in valid 5-minute intervals
        if minute not in self.VALID_INTERVALS:
            return False

        return True


@click.command()
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket name for storing CTS data",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date for date range collection (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date for date range collection (YYYY-MM-DD)",
)
@click.option(
    "--date",
    help="Specific operating day in YYYY-MM-DD format (filters API response)",
)
@click.option(
    "--interval",
    help="Specific 5-minute interval in HH:MM format (00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)",
)
@click.option(
    "--node",
    help="Specific MISO-PJM interface node identifier (e.g., MISO.PJM.INTERFACE1)",
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
@click.option(
    "--force",
    is_flag=True,
    help="Force re-download even if hash exists",
)
def main(
    s3_bucket: str,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    date: Optional[str],
    interval: Optional[str],
    node: Optional[str],
    aws_profile: Optional[str],
    environment: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    log_level: str,
    kafka_connection_string: Optional[str],
    force: bool,
) -> None:
    """Collect MISO Coordinated Transaction Scheduling (CTS) data.

    Examples:
        # Collect current snapshot
        python scraper_miso_coordinated_transaction_scheduling.py --s3-bucket scraper-testing

        # Collect specific date
        python scraper_miso_coordinated_transaction_scheduling.py --s3-bucket scraper-testing --date 2025-01-20

        # Collect date range
        python scraper_miso_coordinated_transaction_scheduling.py \\
            --s3-bucket scraper-testing \\
            --start-date 2025-01-20 \\
            --end-date 2025-01-21

        # Collect with filters
        python scraper_miso_coordinated_transaction_scheduling.py \\
            --s3-bucket scraper-testing \\
            --date 2025-01-20 \\
            --interval 14:00 \\
            --node MISO.PJM.INTERFACE1
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO CTS collection")
    logger.info(f"S3 Bucket: {s3_bucket}")
    logger.info(f"Environment: {environment}")
    logger.info(f"AWS Profile: {aws_profile}")

    # Validate date range
    if start_date and end_date:
        if start_date > end_date:
            logger.error("start-date must be <= end-date")
            raise click.Abort()

    # Set AWS profile if specified
    if aws_profile:
        os.environ["AWS_PROFILE"] = aws_profile
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
    collector = MisoCtsCollector(
        dgroup="miso_coordinated_transaction_scheduling",
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

    # Prepare collection parameters
    collection_params: Dict[str, Any] = {
        "date_param": date,
        "interval": interval,
        "node": node,
    }

    if start_date and end_date:
        collection_params["start_date"] = start_date.date()
        collection_params["end_date"] = end_date.date()

    # Run collection
    try:
        logger.info("Running collection...")
        results = collector.run_collection(force=force, **collection_params)

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
