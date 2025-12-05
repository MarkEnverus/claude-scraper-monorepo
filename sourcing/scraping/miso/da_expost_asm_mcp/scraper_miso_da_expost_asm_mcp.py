"""MISO Day-Ahead Ex-Post Ancillary Services Market Clearing Prices Scraper.

Collects Day-Ahead Ex-Post Ancillary Services Market (ASM) Clearing Prices from MISO's Pricing API.
Endpoint: https://apim.misoenergy.org/pricing/v1/day-ahead/{date}/asm-expost

Data includes Market Clearing Prices for various ancillary service products (Regulation, Spin,
Supplemental, STR, Ramp-up, Ramp-down) across different reserve zones. Ex-Post prices represent
final, settled prices after the market has cleared.

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP REST API collection using BaseCollector framework
    - Automatic pagination handling
    - API key authentication via Ocp-Apim-Subscription-Key header
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


class MisoDayAheadExPostASMMCPCollector(BaseCollector):
    """Collector for MISO Day-Ahead Ex-Post Ancillary Services Market Clearing Prices."""

    BASE_URL = "https://apim.misoenergy.org/pricing/v1/day-ahead"
    TIMEOUT_SECONDS = 180  # MISO API is very slow, can take 2+ minutes to respond
    VALID_PRODUCTS = ["Regulation", "Spin", "Supplemental", "STR", "Ramp-up", "Ramp-down"]
    VALID_ZONES = [f"Zone {i}" for i in range(1, 9)]  # Zone 1 through Zone 8

    def __init__(self, api_key: str, start_date: datetime, end_date: datetime, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate candidates for each date in the range.

        MISO publishes ASM MCP Ex-Post data daily, available at 2pm EST the day before market date.
        Each day returns paginated JSON with multiple product types and zones.
        Ex-Post prices represent final, settled prices after market clearing.
        """
        candidates = []
        current_date = self.start_date

        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y-%m-%d')  # API expects YYYY-MM-DD
            date_compact = current_date.strftime('%Y%m%d')  # For identifier
            identifier = f"da_expost_asm_mcp_{date_compact}.json"
            url = f"{self.BASE_URL}/{date_str}/asm-expost"

            candidate = DownloadCandidate(
                identifier=identifier,
                source_location=url,
                metadata={
                    "data_type": "da_expost_asm_mcp",
                    "source": "miso",
                    "date": date_str,
                    "date_formatted": date_compact,
                    "market_type": "day_ahead_ancillary_services",
                    "price_type": "ex_post",
                },
                collection_params={
                    "headers": {
                        "Ocp-Apim-Subscription-Key": self.api_key,
                        "Accept": "application/json",
                        "User-Agent": "MISO-DA-ExPost-ASM-MCP-Collector/1.0",
                    },
                    "timeout": self.TIMEOUT_SECONDS,
                    "query_params": {
                        "pageNumber": 1,  # Start with first page
                    }
                },
                file_date=current_date.date(),
            )

            candidates.append(candidate)
            logger.info(f"Generated candidate for date: {current_date.date()}")

            current_date += timedelta(days=1)

        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch JSON data from MISO API with pagination support."""
        logger.info(f"Fetching DA Ex-Post ASM MCP data from {candidate.source_location}")

        all_data = []
        page_number = 1
        has_more_pages = True

        while has_more_pages:
            try:
                # Update page number
                params = candidate.collection_params.get("query_params", {}).copy()
                params["pageNumber"] = page_number

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
                page_number += 1

                if has_more_pages:
                    logger.debug(f"More pages available, fetching page {page_number}")

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 400:
                    logger.error(f"Bad request - invalid date format: {candidate.source_location}")
                elif e.response.status_code == 401:
                    logger.error("Unauthorized - invalid API key")
                elif e.response.status_code == 404:
                    logger.warning(f"No data available for date: {candidate.metadata.get('date')}")
                    # 404 is not an error - data may not exist for this date
                    break
                raise ScrapingError(f"HTTP error fetching ASM MCP data: {e}") from e
            except requests.exceptions.RequestException as e:
                raise ScrapingError(f"Failed to fetch ASM MCP data: {e}") from e
            except json.JSONDecodeError as e:
                raise ScrapingError(f"Invalid JSON response: {e}") from e

        # Combine all data into single response
        combined_response = {
            "data": all_data,
            "total_records": len(all_data),
            "metadata": candidate.metadata
        }

        logger.info(f"Successfully collected {len(all_data)} total records across {page_number - 1} pages")
        return json.dumps(combined_response, indent=2).encode('utf-8')

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of ASM MCP data.

        Expected format:
        {
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2024-01-01T00:00:00Z",
                        "end": "2024-01-02T00:00:00Z",
                        "value": "2024-01-01"
                    },
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": 6.48
                }
            ],
            "total_records": 192
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
            required_fields = ["interval", "timeInterval", "product", "zone", "mcp"]

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

            logger.info(f"Validated {len(data['data'])} records successfully")
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON content: {str(e)}")
            return False
        except KeyError as e:
            logger.error(f"Missing required key during validation: {str(e)}")
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
    """Collect MISO Day-Ahead Ex-Post Ancillary Services Market Clearing Prices.

    This scraper collects ASM MCP Ex-Post data from MISO's Pricing API, including final
    settled prices for various ancillary service products (Regulation, Spin, Supplemental,
    STR, Ramp-up, Ramp-down) across different reserve zones.

    Ex-Post prices represent the final, settled prices after the market has cleared.
    Data is typically available at 2pm EST the day before the market date.

    Examples:

        # Collect data for January 2024
        python scraper_miso_da_expost_asm_mcp.py \\
            --api-key YOUR_KEY \\
            --start-date 2024-01-01 \\
            --end-date 2024-01-31

        # Use environment variables for credentials
        export MISO_API_KEY=your_key
        export S3_BUCKET=your-bucket
        python scraper_miso_da_expost_asm_mcp.py \\
            --start-date 2024-01-01 \\
            --end-date 2024-01-31

        # Force re-download existing data
        python scraper_miso_da_expost_asm_mcp.py \\
            --api-key YOUR_KEY \\
            --start-date 2024-01-01 \\
            --end-date 2024-01-02 \\
            --force
    """
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(
        "Starting MISO DA Ex-Post ASM MCP collection",
        extra={
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
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
    collector = MisoDayAheadExPostASMMCPCollector(
        api_key=api_key,
        start_date=start_date,
        end_date=end_date,
        dgroup="miso_da_expost_asm_mcp",
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
