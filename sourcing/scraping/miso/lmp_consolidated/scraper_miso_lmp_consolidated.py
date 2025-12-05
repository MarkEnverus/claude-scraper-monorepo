"""MISO LMP Consolidated Table Scraper.

Collects consolidated Locational Marginal Pricing data from MISO's Public API.
Endpoint: https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable

This endpoint provides a comprehensive, consolidated view of LMP data across multiple
timeframes in a single API call:
- FiveMinLMP: Real-time 5-minute locational marginal pricing
- HourlyIntegratedLMP: Hourly integrated pricing data
- DayAheadExAnteLMP: Day-ahead ex-ante pricing forecasts

Each section contains pricing nodes with LMP components:
- LMP: Locational Marginal Price (USD/MWh)
- MLC: Marginal Loss Component (transmission losses)
- MCC: Marginal Congestion Component (congestion costs)
- MEC: Marginal Energy Component (base energy cost)

The consolidated table is updated every 5 minutes and provides current/latest data
across all three timeframes. No authentication required (public API).

Data is stored to S3 with timestamp partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP REST API collection using BaseCollector framework
    - No authentication required (public API)
    - Real-time consolidated data across 3 timeframes
    - Redis-based hash deduplication
    - S3 storage with timestamp partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive error handling and validation
    - LMP arithmetic validation (LMP = MEC + MCC + MLC)
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

import json
import logging
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


class MisoLmpConsolidatedCollector(BaseCollector):
    """Collector for MISO LMP Consolidated Table data via Public API."""

    BASE_URL = "https://public-api.misoenergy.org/api/MarketPricing"
    ENDPOINT = "GetLmpConsolidatedTable"
    TIMEOUT_SECONDS = 30  # Public API typically responds quickly

    # No date range support - always returns current/latest consolidated data

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate candidate for current consolidated LMP data.

        MISO's consolidated table endpoint returns the latest data across all three
        timeframes (5-min, hourly, day-ahead) in a single response. No date parameters
        supported - always returns current/latest data.

        Since this is a real-time endpoint with no date parameters, we generate a single
        candidate with timestamp-based identifier for deduplication.
        """
        candidates = []

        # Use current UTC timestamp for identifier
        current_time = datetime.now(UTC)
        timestamp_str = current_time.strftime('%Y%m%d_%H%M%S')
        identifier = f"lmp_consolidated_{timestamp_str}.json"
        url = f"{self.BASE_URL}/{self.ENDPOINT}"

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=url,
            metadata={
                "data_type": "lmp_consolidated",
                "source": "miso",
                "collection_timestamp": current_time.isoformat(),
                "market_types": [
                    "five_min_realtime",
                    "hourly_integrated",
                    "day_ahead_exante"
                ],
                "real_time": True,
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-LMP-Consolidated-Collector/1.0",
                },
                "timeout": self.TIMEOUT_SECONDS,
            },
            file_date=current_time.date(),
        )

        candidates.append(candidate)
        logger.info(f"Generated candidate for consolidated LMP data at {current_time.isoformat()}")

        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch JSON data from MISO Public API.

        The consolidated table endpoint returns a single JSON response containing
        three data sections (FiveMinLMP, HourlyIntegratedLMP, DayAheadExAnteLMP).
        No pagination - all current data returned in one response.
        """
        logger.info(f"Fetching LMP Consolidated Table from {candidate.source_location}")

        try:
            response = requests.get(
                candidate.source_location,
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            # Parse JSON response
            json_data = response.json()

            # Add metadata to response
            json_data["collection_metadata"] = candidate.metadata

            logger.info("Successfully collected consolidated LMP data")

            # Log data section sizes
            five_min_count = len(json_data.get("FiveMinLMP", []))
            hourly_count = len(json_data.get("HourlyIntegratedLMP", []))
            da_count = len(json_data.get("DayAheadExAnteLMP", []))

            logger.info(
                f"Data sections: FiveMinLMP={five_min_count}, "
                f"HourlyIntegratedLMP={hourly_count}, "
                f"DayAheadExAnteLMP={da_count}"
            )

            return json.dumps(json_data, indent=2).encode('utf-8')

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning("No consolidated data available at this time")
                # Return empty structure for 404
                empty_response = {
                    "FiveMinLMP": [],
                    "HourlyIntegratedLMP": [],
                    "DayAheadExAnteLMP": [],
                    "collection_metadata": candidate.metadata,
                    "note": "No data available"
                }
                return json.dumps(empty_response, indent=2).encode('utf-8')
            elif e.response.status_code == 503:
                logger.error("MISO Public API temporarily unavailable")
            raise ScrapingError(f"HTTP error fetching consolidated LMP data: {e}") from e
        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch consolidated LMP data: {e}") from e
        except json.JSONDecodeError as e:
            raise ScrapingError(f"Invalid JSON response: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of consolidated LMP data.

        Expected format:
        {
            "FiveMinLMP": [
                {
                    "Timestamp": "HE 9 INT 15",
                    "Region": "South",
                    "PricingNodes": [
                        {
                            "Name": "EES.EXXOBMT",
                            "LMP": 25.50,
                            "MLC": 2.35,
                            "MCC": 5.75,
                            "MEC": 17.40
                        }
                    ]
                }
            ],
            "HourlyIntegratedLMP": [...],
            "DayAheadExAnteLMP": [...]
        }
        """
        try:
            text_content = content.decode('utf-8')
            data = json.loads(text_content)

            # Check for all three required sections
            required_sections = ["FiveMinLMP", "HourlyIntegratedLMP", "DayAheadExAnteLMP"]
            for section in required_sections:
                if section not in data:
                    logger.error(f"Missing required section: {section}")
                    return False

            # Validate at least one section has data
            total_records = sum(
                len(data.get(section, []))
                for section in required_sections
            )

            if total_records == 0:
                logger.warning("All sections are empty - no data available")
                return True  # Empty is valid for real-time endpoint

            # Validate structure of first non-empty section
            for section_name in required_sections:
                section_data = data.get(section_name, [])
                if not section_data:
                    continue

                # Get first time interval group
                first_group = section_data[0]

                # Validate time interval group structure
                if "Timestamp" not in first_group:
                    logger.error(f"Missing Timestamp in {section_name}")
                    return False

                if "Region" not in first_group:
                    logger.error(f"Missing Region in {section_name}")
                    return False

                if "PricingNodes" not in first_group or not isinstance(first_group["PricingNodes"], list):
                    logger.error(f"Missing or invalid PricingNodes array in {section_name}")
                    return False

                # Validate at least one pricing node
                if len(first_group["PricingNodes"]) == 0:
                    logger.warning(f"No pricing nodes in {section_name}")
                    continue

                # Validate first pricing node structure
                node = first_group["PricingNodes"][0]
                required_node_fields = ["Name", "LMP", "MLC", "MCC"]

                for field in required_node_fields:
                    if field not in node:
                        logger.error(f"Missing required field '{field}' in {section_name} pricing node")
                        return False

                # Validate numeric fields
                for component in ["LMP", "MLC", "MCC"]:
                    if not isinstance(node[component], (int, float)):
                        logger.error(f"{component} value is not numeric in {section_name}: {node[component]}")
                        return False

                # Validate MEC if present (not always included)
                if "MEC" in node:
                    if not isinstance(node["MEC"], (int, float)):
                        logger.error(f"MEC value is not numeric in {section_name}: {node['MEC']}")
                        return False

                    # Validate LMP arithmetic: LMP = MEC + MCC + MLC (within tolerance)
                    calculated_lmp = node["MEC"] + node["MCC"] + node["MLC"]
                    if abs(calculated_lmp - node["LMP"]) > 0.01:
                        logger.warning(
                            f"LMP arithmetic mismatch in {section_name} for node {node['Name']}: "
                            f"LMP={node['LMP']}, MEC+MCC+MLC={calculated_lmp:.2f}"
                        )

                # Validate timestamp format
                timestamp = first_group["Timestamp"]
                if section_name == "FiveMinLMP":
                    # Format: "HE X INT Y"
                    if not (timestamp.startswith("HE ") and "INT " in timestamp):
                        logger.warning(f"Unexpected FiveMinLMP timestamp format: {timestamp}")
                elif section_name == "HourlyIntegratedLMP":
                    # Format: "HE X"
                    if not timestamp.startswith("HE "):
                        logger.warning(f"Unexpected HourlyIntegratedLMP timestamp format: {timestamp}")
                # DayAheadExAnteLMP format can vary

                logger.info(f"Validated {len(first_group['PricingNodes'])} nodes in {section_name}")
                break  # Only need to validate one section structure

            logger.info(f"Successfully validated consolidated LMP data with {total_records} total records")
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
    help="Force re-download even if content hash exists"
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
    """Collect MISO LMP Consolidated Table data.

    This scraper collects consolidated Locational Marginal Price data from MISO's
    Public API, including real-time 5-minute, hourly integrated, and day-ahead
    ex-ante pricing across all MISO regions and pricing nodes.

    The endpoint returns current/latest data across all three timeframes in a single
    API call. No authentication required (public API).

    Data sections:
    - FiveMinLMP: Real-time 5-minute LMP data (HE X INT Y format)
    - HourlyIntegratedLMP: Hourly integrated LMP data (HE X format)
    - DayAheadExAnteLMP: Day-ahead ex-ante forecasted LMP data

    Each section contains pricing nodes with LMP components (LMP, MLC, MCC, MEC).

    Examples:

        # Collect current consolidated LMP data
        python scraper_miso_lmp_consolidated.py

        # Use environment variables for AWS/Redis
        export S3_BUCKET=your-bucket
        export REDIS_HOST=localhost
        python scraper_miso_lmp_consolidated.py

        # Force re-download even if content unchanged
        python scraper_miso_lmp_consolidated.py --force

        # Debug logging for troubleshooting
        python scraper_miso_lmp_consolidated.py --log-level DEBUG

        # Use specific AWS profile
        python scraper_miso_lmp_consolidated.py \\
            --aws-profile production \\
            --environment prod
    """
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(
        "Starting MISO LMP Consolidated Table collection",
        extra={
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
    collector = MisoLmpConsolidatedCollector(
        dgroup="miso_lmp_consolidated",
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
                "files_downloaded": results.get("files_downloaded", 0),
                "files_skipped": results.get("files_skipped", 0),
                "files_failed": results.get("files_failed", 0),
                "total_processed": results.get("total_processed", 0)
            }
        )

    except Exception as e:
        logger.error(f"Collection failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
