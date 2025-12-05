"""MISO Public Ancillary Services Market Clearing Price (MCP) Scraper.

Collects real-time and day-ahead reserve market clearing prices from MISO's public API.
API: https://public-api.misoenergy.org/api/MarketPricing/GetAncillaryServicesMcp

This scraper collects Market Clearing Prices (MCP) for ancillary services markets including:
- Regulation reserves
- Spinning reserves (Spin)
- Supplemental reserves
- Short-term reserves (STR)
- Ramp-up capability
- Ramp-down capability

Data is available for 8 reserve zones and updates every 5 minutes for both real-time
and day-ahead market prices.

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP collection using BaseCollector framework
    - Optional query parameter filtering (product, zone, datetime)
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive validation (product, zone, price values)
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

import json
import logging
import os
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


class MisoPublicASMMCPCollector(BaseCollector):
    """Collector for MISO Public Ancillary Services Market Clearing Prices."""

    API_URL = "https://public-api.misoenergy.org/api/MarketPricing/GetAncillaryServicesMcp"
    TIMEOUT_SECONDS = 30

    # Valid product names (case-sensitive as per MISO API)
    VALID_PRODUCTS = {
        "Regulation",
        "Spin",
        "Supplemental",
        "STR",
        "Ramp-up",
        "Ramp-down"
    }

    # Valid reserve zones
    VALID_ZONES = {
        "Zone 1", "Zone 2", "Zone 3", "Zone 4",
        "Zone 5", "Zone 6", "Zone 7", "Zone 8"
    }

    def __init__(
        self,
        product: Optional[str] = None,
        zone: Optional[str] = None,
        specific_datetime: Optional[str] = None,
        **kwargs
    ):
        """Initialize collector with optional query parameters.

        Args:
            product: Optional product filter (Regulation, Spin, Supplemental, STR, Ramp-up, Ramp-down)
            zone: Optional zone filter (Zone 1 through Zone 8)
            specific_datetime: Optional ISO 8601 datetime filter
            **kwargs: BaseCollector arguments
        """
        super().__init__(**kwargs)
        self.product = product
        self.zone = zone
        self.specific_datetime = specific_datetime

        # Validate parameters
        if product and product not in self.VALID_PRODUCTS:
            raise ValueError(
                f"Invalid product '{product}'. Must be one of: {', '.join(sorted(self.VALID_PRODUCTS))}"
            )
        if zone and zone not in self.VALID_ZONES:
            raise ValueError(
                f"Invalid zone '{zone}'. Must be one of: {', '.join(sorted(self.VALID_ZONES))}"
            )

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate single candidate for current ASM MCP snapshot.

        MISO API provides a snapshot of current market clearing prices across all
        products and zones, updating every 5 minutes. Optional filters can be applied
        to narrow results by product, zone, or datetime.
        """
        collection_time = datetime.now(UTC)

        # Build identifier based on filters
        identifier_parts = ["public_asm_mcp"]
        if self.product:
            identifier_parts.append(self.product.lower().replace("-", "_"))
        if self.zone:
            identifier_parts.append(f"zone{self.zone.split()[-1]}")
        if self.specific_datetime:
            # Extract date from ISO datetime for identifier
            try:
                dt = datetime.fromisoformat(self.specific_datetime.replace('Z', '+00:00'))
                identifier_parts.append(dt.strftime('%Y%m%d_%H%M'))
            except Exception:
                pass
        else:
            identifier_parts.append(collection_time.strftime('%Y%m%d_%H%M'))

        identifier = f"{'_'.join(identifier_parts)}.json"

        # Build query parameters
        query_params = {}
        if self.product:
            query_params["product"] = self.product
        if self.zone:
            query_params["zone"] = self.zone
        if self.specific_datetime:
            query_params["datetime"] = self.specific_datetime

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=self.API_URL,
            metadata={
                "data_type": "public_asm_mcp",
                "source": "miso",
                "collection_timestamp": collection_time.isoformat(),
                "product_filter": self.product,
                "zone_filter": self.zone,
                "datetime_filter": self.specific_datetime,
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-Public-ASM-MCP-Collector/1.0",
                },
                "timeout": self.TIMEOUT_SECONDS,
                "query_params": query_params,
            },
            file_date=collection_time.date(),
        )

        logger.info(f"Generated candidate: {identifier}")
        if query_params:
            logger.info(f"Query filters: {query_params}")

        return [candidate]

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch ASM MCP data from MISO API."""
        logger.info(f"Fetching ASM MCP data from {candidate.source_location}")

        query_params = candidate.collection_params.get("query_params", {})
        if query_params:
            logger.debug(f"Using query parameters: {query_params}")

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

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                logger.error(f"Bad request - invalid query parameters: {query_params}")
            elif e.response.status_code == 404:
                logger.warning("No data available for specified filters")
            raise ScrapingError(f"Failed to fetch ASM MCP data: {e}") from e
        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch ASM MCP data: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure and data integrity of ASM MCP response.

        Expected structure:
        {
            "data": [
                {
                    "datetime": "2025-12-05T10:30:00Z",
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": 5.23,
                    "marketType": "real-time"
                }
            ],
            "metadata": {
                "totalRecords": 96,
                "updateTimestamp": "2025-12-05T10:35:00Z",
                "dataSource": "MISO Public Market Pricing"
            }
        }
        """
        try:
            data = json.loads(content)

            # Check for required top-level fields
            if "data" not in data:
                logger.error("Missing required field: 'data'")
                return False

            if "metadata" not in data:
                logger.error("Missing required field: 'metadata'")
                return False

            # Validate metadata structure
            metadata = data["metadata"]
            required_metadata_fields = ["totalRecords", "updateTimestamp", "dataSource"]
            for field in required_metadata_fields:
                if field not in metadata:
                    logger.warning(f"Missing metadata field: {field}")

            # Empty data array is valid (no records for filters)
            if not isinstance(data["data"], list):
                logger.error("'data' field must be an array")
                return False

            if len(data["data"]) == 0:
                logger.warning("Empty data array - no records matching filters")
                return True

            # Validate totalRecords matches actual data length
            if "totalRecords" in metadata:
                expected_count = metadata["totalRecords"]
                actual_count = len(data["data"])
                if expected_count != actual_count:
                    logger.error(
                        f"Record count mismatch: metadata says {expected_count}, "
                        f"but data array has {actual_count} records"
                    )
                    return False

            # Validate first record structure
            first_record = data["data"][0]
            required_record_fields = ["datetime", "product", "zone", "mcp", "marketType"]
            for field in required_record_fields:
                if field not in first_record:
                    logger.error(f"Missing required field in data record: {field}")
                    return False

            # Validate product value
            product = first_record["product"]
            if product not in self.VALID_PRODUCTS:
                logger.error(
                    f"Invalid product value: '{product}'. "
                    f"Must be one of: {', '.join(sorted(self.VALID_PRODUCTS))}"
                )
                return False

            # Validate zone value
            zone = first_record["zone"]
            if zone not in self.VALID_ZONES:
                logger.error(
                    f"Invalid zone value: '{zone}'. "
                    f"Must be one of: {', '.join(sorted(self.VALID_ZONES))}"
                )
                return False

            # Validate MCP is non-negative number
            mcp = first_record["mcp"]
            if not isinstance(mcp, (int, float)):
                logger.error(f"MCP must be a number, got: {type(mcp)}")
                return False
            if mcp < 0:
                logger.error(f"MCP must be non-negative, got: {mcp}")
                return False

            # Validate marketType
            market_type = first_record["marketType"]
            if market_type not in ["real-time", "day-ahead"]:
                logger.error(f"Invalid marketType: '{market_type}'. Must be 'real-time' or 'day-ahead'")
                return False

            # Validate datetime format (ISO 8601)
            try:
                datetime.fromisoformat(first_record["datetime"].replace('Z', '+00:00'))
            except ValueError as e:
                logger.error(f"Invalid datetime format: {first_record['datetime']}: {e}")
                return False

            # Log successful validation with statistics
            record_count = len(data["data"])
            products = set(r["product"] for r in data["data"])
            zones = set(r["zone"] for r in data["data"])
            market_types = set(r["marketType"] for r in data["data"])

            logger.info(
                f"Content validation passed: {record_count} records, "
                f"Products: {len(products)}, Zones: {len(zones)}, "
                f"Market Types: {', '.join(market_types)}"
            )

            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return False
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Validation error: {e}")
            return False


@click.command()
@click.option(
    "--product",
    type=click.Choice(["Regulation", "Spin", "Supplemental", "STR", "Ramp-up", "Ramp-down"]),
    help="Filter by reserve product (optional)",
)
@click.option(
    "--zone",
    type=click.Choice([f"Zone {i}" for i in range(1, 9)]),
    help="Filter by reserve zone (optional)",
)
@click.option(
    "--datetime",
    "specific_datetime",
    help="Filter by specific ISO 8601 datetime (optional)",
)
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket name for storing ASM MCP data",
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
    product: Optional[str],
    zone: Optional[str],
    specific_datetime: Optional[str],
    s3_bucket: str,
    aws_profile: Optional[str],
    environment: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    log_level: str,
    kafka_connection_string: Optional[str],
):
    """Collect MISO Public Ancillary Services Market Clearing Price data.

    This scraper collects Market Clearing Prices (MCP) for ancillary services markets
    including regulation, spinning, supplemental, short-term, and ramp reserves.

    Data updates every 5 minutes and includes both real-time and day-ahead market prices
    across 8 reserve zones.

    Optional filters can narrow results by product, zone, or datetime.

    Examples:

        # Collect all ASM MCP data (all products, all zones)
        python scraper_miso_public_asm_mcp.py --s3-bucket scraper-testing

        # Collect only Regulation reserves
        python scraper_miso_public_asm_mcp.py --s3-bucket scraper-testing --product Regulation

        # Collect Zone 1 prices
        python scraper_miso_public_asm_mcp.py --s3-bucket scraper-testing --zone "Zone 1"

        # Collect Spin reserves for Zone 3
        python scraper_miso_public_asm_mcp.py --s3-bucket scraper-testing --product Spin --zone "Zone 3"

        # Collect specific datetime
        python scraper_miso_public_asm_mcp.py --s3-bucket scraper-testing --datetime 2025-12-05T10:30:00Z

        # Use with LocalStack
        python scraper_miso_public_asm_mcp.py --s3-bucket scraper-testing --aws-profile localstack
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO Public ASM MCP collection")
    logger.info(f"S3 Bucket: {s3_bucket}")
    logger.info(f"Environment: {environment}")
    logger.info(f"AWS Profile: {aws_profile}")
    if product:
        logger.info(f"Product Filter: {product}")
    if zone:
        logger.info(f"Zone Filter: {zone}")
    if specific_datetime:
        logger.info(f"Datetime Filter: {specific_datetime}")

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
    try:
        collector = MisoPublicASMMCPCollector(
            product=product,
            zone=zone,
            specific_datetime=specific_datetime,
            dgroup="miso_public_asm_mcp",
            s3_bucket=s3_bucket,
            s3_prefix="sourcing",
            redis_client=redis_client,
            environment=environment,
            kafka_connection_string=kafka_connection_string,
        )
    except ValueError as e:
        logger.error(f"Invalid parameters: {e}")
        raise click.Abort()

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
        logger.error(f"Collection failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
