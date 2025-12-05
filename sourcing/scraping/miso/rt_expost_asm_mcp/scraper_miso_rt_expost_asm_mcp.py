"""MISO Real-Time Ex-Post Ancillary Services Market Clearing Prices Scraper.

Collects Real-Time Ex-Post Ancillary Services Market (ASM) Clearing Prices from MISO's Pricing API.
Endpoint: https://apim.misoenergy.org/pricing/v1/real-time/{date}/asm-expost

Ex-Post MCP data represents final settled market clearing prices for the real-time ancillary
services market across MISO's reserve zones. This data is crucial for financial settlement,
market analysis, and performance evaluation. Data is available at 7:00 AM EST on the day after
the operating day, with preliminary prices that finalize within 3-5 days.

Data includes Market Clearing Prices for various ancillary service products (Regulation, Spin,
Supplemental, STR, Ramp-up, Ramp-down) across 8 reserve zones, with support for both 5-minute
and hourly time resolutions.

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP REST API collection using BaseCollector framework
    - Automatic pagination handling
    - API key authentication via Ocp-Apim-Subscription-Key header
    - Support for preliminary and final price states
    - Support for multiple ancillary service products
    - Support for 5-minute and hourly time resolutions
    - Support for 8 reserve zones
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive error handling and validation
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

import json
import logging
from datetime import datetime, timedelta
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


class MisoRealTimeExPostASMMCPCollector(BaseCollector):
    """Collector for MISO Real-Time Ex-Post Ancillary Services Market Clearing Prices."""

    BASE_URL = "https://apim.misoenergy.org/pricing/v1/real-time"
    TIMEOUT_SECONDS = 180  # MISO API can be slow with large paginated responses
    VALID_PRODUCTS = ["Regulation", "Spin", "Supplemental", "STR", "Ramp-up", "Ramp-down"]
    VALID_ZONES = [f"Zone {i}" for i in range(1, 9)]  # Zone 1 through Zone 8
    VALID_PRELIMINARY_FINAL = ["Preliminary", "Final"]
    VALID_TIME_RESOLUTIONS = ["5min", "hourly"]

    def __init__(
        self,
        api_key: str,
        start_date: datetime,
        end_date: datetime,
        product: Optional[str] = None,
        zone: Optional[str] = None,
        preliminary_final: Optional[str] = None,
        time_resolution: str = "hourly",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date
        self.product = product
        self.zone = zone
        self.preliminary_final = preliminary_final
        self.time_resolution = time_resolution

        # Validate parameters
        if self.product and self.product not in self.VALID_PRODUCTS:
            raise ValueError(f"Invalid product: {self.product}. Must be one of {self.VALID_PRODUCTS}")
        if self.zone and self.zone not in self.VALID_ZONES:
            raise ValueError(f"Invalid zone: {self.zone}. Must be one of {self.VALID_ZONES}")
        if self.preliminary_final and self.preliminary_final not in self.VALID_PRELIMINARY_FINAL:
            raise ValueError(
                f"Invalid preliminaryFinal: {self.preliminary_final}. "
                f"Must be one of {self.VALID_PRELIMINARY_FINAL}"
            )
        if self.time_resolution not in self.VALID_TIME_RESOLUTIONS:
            raise ValueError(
                f"Invalid time resolution: {self.time_resolution}. "
                f"Must be one of {self.VALID_TIME_RESOLUTIONS}"
            )

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate candidates for each date in the range.

        MISO publishes RT Ex-Post ASM MCP data daily at 7am EST on the day after the
        operating day. Each day returns paginated JSON with multiple product types and zones.
        Data progresses from preliminary to final state within 3-5 days.
        """
        candidates = []
        current_date = self.start_date

        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y-%m-%d')  # API expects YYYY-MM-DD
            date_compact = current_date.strftime('%Y%m%d')  # For identifier

            # Build identifier with optional filters
            identifier_parts = ["rt_expost_asm_mcp", date_compact]
            if self.preliminary_final:
                identifier_parts.append(self.preliminary_final.lower())
            if self.product:
                identifier_parts.append(self.product.lower().replace("-", "_"))
            if self.zone:
                identifier_parts.append(f"zone{self.zone.split()[-1]}")
            identifier_parts.append(self.time_resolution)
            identifier = "_".join(identifier_parts) + ".json"

            url = f"{self.BASE_URL}/{date_str}/asm-expost"

            # Build query parameters
            query_params = {
                "pageNumber": 1,
                "timeResolution": self.time_resolution,
            }
            if self.product:
                query_params["product"] = self.product
            if self.zone:
                query_params["zone"] = self.zone
            if self.preliminary_final:
                query_params["preliminaryFinal"] = self.preliminary_final

            candidate = DownloadCandidate(
                identifier=identifier,
                source_location=url,
                metadata={
                    "data_type": "rt_expost_asm_mcp",
                    "source": "miso",
                    "date": date_str,
                    "date_formatted": date_compact,
                    "market_type": "real_time_ancillary_services_expost",
                    "product": self.product,
                    "zone": self.zone,
                    "preliminary_final": self.preliminary_final,
                    "time_resolution": self.time_resolution,
                },
                collection_params={
                    "headers": {
                        "Ocp-Apim-Subscription-Key": self.api_key,
                        "Accept": "application/json",
                        "User-Agent": "MISO-RT-ExPost-ASM-MCP-Collector/1.0",
                    },
                    "timeout": self.TIMEOUT_SECONDS,
                    "query_params": query_params,
                },
                file_date=current_date.date(),
            )

            candidates.append(candidate)
            logger.info(f"Generated candidate for date: {current_date.date()}")

            current_date += timedelta(days=1)

        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch JSON data from MISO API with pagination support.

        The RT Ex-Post ASM MCP endpoint returns paginated data. Volume varies based on:
        - Time resolution (5-minute vs hourly)
        - Product filter (single product vs all 6)
        - Zone filter (single zone vs all 8)
        - Preliminary/Final state
        """
        logger.info(f"Fetching RT Ex-Post ASM MCP data from {candidate.source_location}")

        all_data = []
        page_number = 1
        has_more_pages = True
        total_pages = None

        while has_more_pages:
            try:
                # Update page number
                params = candidate.collection_params.get("query_params", {}).copy()
                params["pageNumber"] = page_number

                logger.debug(f"Requesting page {page_number}" + (f" of {total_pages}" if total_pages else ""))

                response = requests.get(
                    candidate.source_location,
                    params=params,
                    headers=candidate.collection_params.get("headers", {}),
                    timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
                )
                response.raise_for_status()

                # Parse JSON response
                json_data = response.json()

                # Extract data records
                if "data" in json_data and json_data["data"]:
                    all_data.extend(json_data["data"])
                    logger.info(f"Collected {len(json_data['data'])} records from page {page_number}")

                # Check pagination
                page_info = json_data.get("page", {})
                has_more_pages = not page_info.get("lastPage", True)

                # Track total pages for progress logging
                if total_pages is None and "totalPages" in page_info:
                    total_pages = page_info["totalPages"]
                    logger.info(f"Total pages to fetch: {total_pages}")

                page_number += 1

                if has_more_pages:
                    logger.debug(f"More pages available, fetching page {page_number}")

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 400:
                    logger.error(f"Bad request - invalid parameters: {candidate.source_location}")
                elif e.response.status_code == 401:
                    logger.error("Unauthorized - invalid API key")
                elif e.response.status_code == 404:
                    logger.warning(f"No data available for date: {candidate.metadata.get('date')}")
                    # 404 is not an error - data may not exist for this date yet
                    break
                elif e.response.status_code == 429:
                    logger.warning("Rate limit exceeded - consider adding delays between requests")
                raise ScrapingError(f"HTTP error fetching RT Ex-Post ASM MCP data: {e}") from e
            except requests.exceptions.RequestException as e:
                raise ScrapingError(f"Failed to fetch RT Ex-Post ASM MCP data: {e}") from e
            except json.JSONDecodeError as e:
                raise ScrapingError(f"Invalid JSON response: {e}") from e

        # Combine all data into single response
        combined_response = {
            "data": all_data,
            "total_records": len(all_data),
            "total_pages": page_number - 1,
            "metadata": candidate.metadata
        }

        logger.info(f"Successfully collected {len(all_data)} total records across {page_number - 1} pages")
        return json.dumps(combined_response, indent=2).encode('utf-8')

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of RT Ex-Post ASM MCP data.

        Expected format:
        {
            "data": [
                {
                    "preliminaryFinal": "Preliminary",
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": 6.48,
                    "timeInterval": {
                        "resolution": "5min",
                        "start": "2023-06-28 00:00:00.000",
                        "end": "2023-06-29 00:00:00.000",
                        "value": "2023-06-29"
                    }
                }
            ],
            "total_records": 288
        }
        """
        try:
            text_content = content.decode('utf-8')
            data = json.loads(text_content)

            # Check top-level structure
            if "data" not in data:
                logger.error("Missing 'data' field in response")
                return False

            # Empty data is valid (no data available for date)
            if not data["data"] or len(data["data"]) == 0:
                logger.warning(f"No data records for {candidate.metadata.get('date')}")
                return True

            # Validate first record structure
            record = data["data"][0]
            required_fields = ["preliminaryFinal", "product", "zone", "mcp", "timeInterval"]

            for field in required_fields:
                if field not in record:
                    logger.error(f"Missing required field: {field}")
                    return False

            # Validate timeInterval structure
            time_interval = record["timeInterval"]
            required_time_fields = ["resolution", "start", "end", "value"]
            for field in required_time_fields:
                if field not in time_interval:
                    logger.error(f"Missing required timeInterval field: {field}")
                    return False

            # Validate preliminaryFinal value
            if record["preliminaryFinal"] not in self.VALID_PRELIMINARY_FINAL:
                logger.warning(f"Unexpected preliminaryFinal value: {record['preliminaryFinal']}")

            # Validate product type
            if record["product"] not in self.VALID_PRODUCTS:
                logger.warning(f"Unexpected product value: {record['product']}")

            # Validate zone format
            if record["zone"] not in self.VALID_ZONES:
                logger.warning(f"Unexpected zone value: {record['zone']}")

            # Validate mcp is numeric
            if not isinstance(record["mcp"], (int, float)):
                logger.error(f"MCP value is not numeric: {record['mcp']}")
                return False

            # Validate time resolution matches request
            expected_resolution = candidate.metadata.get("time_resolution", "hourly")
            if time_interval.get("resolution") != expected_resolution:
                logger.warning(
                    f"Time resolution mismatch: expected {expected_resolution}, "
                    f"got {time_interval.get('resolution')}"
                )

            # Validate date consistency
            expected_date = candidate.metadata.get('date')
            if time_interval.get('value') != expected_date:
                logger.error(
                    f"Date mismatch: expected {expected_date}, got {time_interval.get('value')}"
                )
                return False

            # Check for reasonable data volume
            record_count = len(data["data"])
            logger.info(f"Validated {record_count} records successfully")

            # Expected volumes:
            # - Hourly: 6 products × 8 zones × 24 hours = ~1,152 records (preliminary + final)
            # - 5-minute: 6 products × 8 zones × 288 intervals = ~13,824 records (preliminary + final)
            # But allow smaller counts for filtered queries
            if record_count > 0 and record_count < 10:
                logger.warning(f"Unexpectedly low record count: {record_count}")

            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON content: {str(e)}")
            return False
        except (KeyError, ValueError) as e:
            logger.error(f"Validation error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected validation error: {str(e)}")
            return False


@click.command()
@click.option(
    "--api-key",
    required=True,
    envvar="MISO_API_KEY",
    help="MISO API subscription key (or set MISO_API_KEY environment variable)"
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Start date for data collection (YYYY-MM-DD)"
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="End date for data collection (YYYY-MM-DD)"
)
@click.option(
    "--product",
    type=click.Choice(["Regulation", "Spin", "Supplemental", "STR", "Ramp-up", "Ramp-down"]),
    help="Filter by specific ancillary service product (optional)"
)
@click.option(
    "--zone",
    type=click.Choice([f"Zone {i}" for i in range(1, 9)]),
    help="Filter by specific reserve zone (optional, Zone 1-8)"
)
@click.option(
    "--preliminary-final",
    type=click.Choice(["Preliminary", "Final"]),
    help="Filter by price state: Preliminary or Final (optional)"
)
@click.option(
    "--time-resolution",
    type=click.Choice(["5min", "hourly"]),
    default="hourly",
    help="Time resolution: 5-minute or hourly (default: hourly)"
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
    api_key: str,
    start_date: datetime,
    end_date: datetime,
    product: Optional[str],
    zone: Optional[str],
    preliminary_final: Optional[str],
    time_resolution: str,
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
    """Collect MISO Real-Time Ex-Post Ancillary Services Market Clearing Prices.

    This scraper collects RT Ex-Post ASM MCP data from MISO's Pricing API, including
    settled market clearing prices for various ancillary service products (Regulation,
    Spin, Supplemental, STR, Ramp-up, Ramp-down) across 8 reserve zones.

    Data is available at 7:00 AM EST on the day after the operating day. Prices
    initially appear as Preliminary and finalize within 3-5 days.

    Examples:

        # Collect all data for a date range (hourly resolution)
        python scraper_miso_rt_expost_asm_mcp.py \\
            --api-key YOUR_KEY \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-31

        # Collect only final prices at 5-minute resolution
        python scraper_miso_rt_expost_asm_mcp.py \\
            --api-key YOUR_KEY \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-31 \\
            --preliminary-final Final \\
            --time-resolution 5min

        # Collect specific product and zone
        python scraper_miso_rt_expost_asm_mcp.py \\
            --api-key YOUR_KEY \\
            --start-date 2025-01-20 \\
            --end-date 2025-01-20 \\
            --product Regulation \\
            --zone "Zone 1"

        # Use environment variables for credentials
        export MISO_API_KEY=your_key
        export S3_BUCKET=your-bucket
        python scraper_miso_rt_expost_asm_mcp.py \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-31

        # Force re-download existing data with debug logging
        python scraper_miso_rt_expost_asm_mcp.py \\
            --api-key YOUR_KEY \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-02 \\
            --force \\
            --log-level DEBUG
    """
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(
        "Starting MISO RT Ex-Post ASM MCP collection",
        extra={
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "product": product,
            "zone": zone,
            "preliminary_final": preliminary_final,
            "time_resolution": time_resolution,
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
    collector = MisoRealTimeExPostASMMCPCollector(
        api_key=api_key,
        start_date=start_date,
        end_date=end_date,
        product=product,
        zone=zone,
        preliminary_final=preliminary_final,
        time_resolution=time_resolution,
        dgroup="miso_rt_expost_asm_mcp",
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
