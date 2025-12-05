"""MISO Real-Time Ex-Ante LMP Scraper.

Collects Real-Time Ex-Ante (forecasted) Locational Marginal Pricing data from MISO's Pricing API.
Endpoint: https://apim.misoenergy.org/pricing/v1/real-time/{date}/lmp-exante

Ex-Ante LMP data represents forecasted prices for the real-time energy market across
MISO's commercial pricing nodes. This data is published at 7:00 AM EST on the operating
day and is continuously updated throughout the day for near real-time trading, grid
operations, and predictive analysis.

Key differences from Day-Ahead Ex-Ante:
- Published 7:00 AM EST on operating day (vs 2:00 PM EST day before)
- Can be 5-minute intervals (288 records/day) OR hourly (24 records/day)
- Higher data volume: ~300K-400K records/day for 5-min, ~72K-120K for hourly
- Near real-time updates throughout operating day

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP REST API collection using BaseCollector framework
    - Automatic pagination handling for high-volume data
    - Configurable time resolution (5-minute or hourly)
    - API key authentication via Ocp-Apim-Subscription-Key header
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive error handling and validation
    - LMP arithmetic validation (LMP = MEC + MCC + MLC)
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


class MisoRealTimeExAnteLMPCollector(BaseCollector):
    """Collector for MISO Real-Time Ex-Ante LMP data via Pricing API."""

    BASE_URL = "https://apim.misoenergy.org/pricing/v1/real-time"
    TIMEOUT_SECONDS = 240  # Real-time data can be large, especially 5-minute intervals

    # Expected data volume depends on time resolution:
    # - Hourly: ~3,000-5,000 nodes × 24 intervals = ~72,000-120,000 records per day
    # - 5-minute: ~3,000-5,000 nodes × 288 intervals = ~300,000-400,000 records per day

    def __init__(
        self,
        api_key: str,
        start_date: datetime,
        end_date: datetime,
        time_resolution: str = "hourly",
        **kwargs
    ):
        """Initialize Real-Time Ex-Ante LMP collector.

        Args:
            api_key: MISO API subscription key
            start_date: Start date for data collection
            end_date: End date for data collection
            time_resolution: "5min" or "hourly" (default: hourly)
            **kwargs: Additional arguments passed to BaseCollector
        """
        super().__init__(**kwargs)
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date

        # Validate and normalize time resolution
        if time_resolution.lower() in ["5min", "5-min", "5minute", "5-minute"]:
            self.time_resolution = "5min"
        elif time_resolution.lower() in ["hourly", "hour", "1hour"]:
            self.time_resolution = "hourly"
        else:
            raise ValueError(
                f"Invalid time_resolution: {time_resolution}. Must be '5min' or 'hourly'"
            )

        logger.info(f"Initialized RT Ex-Ante LMP collector with {self.time_resolution} resolution")

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate candidates for each date in the range.

        MISO publishes Ex-Ante LMP data throughout the operating day, first available
        at 7:00 AM EST. Each day returns paginated JSON with forecasted LMP data
        for all commercial pricing nodes (CPNodes) at either 5-minute or hourly intervals.
        """
        candidates = []
        current_date = self.start_date

        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y-%m-%d')  # API expects YYYY-MM-DD
            date_compact = current_date.strftime('%Y%m%d')  # For identifier
            identifier = f"rt_exante_lmp_{self.time_resolution}_{date_compact}.json"
            url = f"{self.BASE_URL}/{date_str}/lmp-exante"

            candidate = DownloadCandidate(
                identifier=identifier,
                source_location=url,
                metadata={
                    "data_type": "rt_exante_lmp",
                    "source": "miso",
                    "date": date_str,
                    "date_formatted": date_compact,
                    "market_type": "real_time_energy_exante",
                    "time_resolution": self.time_resolution,
                    "forecast": True,  # Key distinction: forecasted prices
                },
                collection_params={
                    "headers": {
                        "Ocp-Apim-Subscription-Key": self.api_key,
                        "Accept": "application/json",
                        "User-Agent": "MISO-RT-ExAnte-LMP-Collector/1.0",
                    },
                    "timeout": self.TIMEOUT_SECONDS,
                    "query_params": {
                        "pageNumber": 1,  # Start with first page
                        "timeResolution": self.time_resolution,  # 5min or hourly
                    }
                },
                file_date=current_date.date(),
            )

            candidates.append(candidate)
            logger.info(
                f"Generated candidate for date: {current_date.date()} "
                f"({self.time_resolution} resolution)"
            )

            current_date += timedelta(days=1)

        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch JSON data from MISO API with pagination support.

        The Ex-Ante LMP endpoint returns paginated data with potentially hundreds of pages.
        Expected volume:
        - Hourly: ~72,000-120,000 forecasted records per day
        - 5-minute: ~300,000-400,000 forecasted records per day
        """
        time_res = candidate.metadata.get("time_resolution", "hourly")
        logger.info(
            f"Fetching RT Ex-Ante LMP data ({time_res}) from {candidate.source_location}"
        )

        all_data = []
        page_number = 1
        has_more_pages = True
        total_pages = None

        while has_more_pages:
            try:
                # Update page number
                params = candidate.collection_params.get("query_params", {}).copy()
                params["pageNumber"] = page_number

                logger.debug(
                    f"Requesting page {page_number}" + (f" of {total_pages}" if total_pages else "")
                )

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
                    # 404 is not an error - forecast data may not exist for this date yet
                    break
                elif e.response.status_code == 429:
                    logger.warning("Rate limit exceeded - consider adding delays between requests")
                raise ScrapingError(f"HTTP error fetching RT Ex-Ante LMP data: {e}") from e
            except requests.exceptions.RequestException as e:
                raise ScrapingError(f"Failed to fetch RT Ex-Ante LMP data: {e}") from e
            except json.JSONDecodeError as e:
                raise ScrapingError(f"Invalid JSON response: {e}") from e

        # Combine all data into single response
        combined_response = {
            "data": all_data,
            "total_records": len(all_data),
            "total_pages": page_number - 1,
            "time_resolution": time_res,
            "metadata": candidate.metadata
        }

        logger.info(
            f"Successfully collected {len(all_data)} total records "
            f"across {page_number - 1} pages ({time_res})"
        )
        return json.dumps(combined_response, indent=2).encode('utf-8')

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of Real-Time Ex-Ante LMP data.

        Expected format:
        {
            "data": [
                {
                    "node": "ALTW.WELLS1",
                    "lmp": 22.1,    // Forecasted real-time price
                    "mcc": 0.03,    // Forecasted congestion cost
                    "mec": 21.34,   // Forecasted energy cost
                    "mlc": 0.73,    // Forecasted loss cost
                    "interval": "1" or "00:05",  // Depends on resolution
                    "timeInterval": {
                        "resolution": "5min" or "hourly",
                        "start": "2023-06-29 00:00:00.000",
                        "end": "2023-06-29 00:05:00.000",
                        "value": "2023-06-29"
                    }
                }
            ],
            "total_records": 300000,
            "time_resolution": "5min"
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
            required_fields = ["node", "lmp", "mcc", "mec", "mlc", "interval", "timeInterval"]

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

            # Validate interval based on resolution
            time_res = candidate.metadata.get("time_resolution", "hourly")
            interval_value = record["interval"]

            if time_res == "hourly":
                # Hourly: interval should be 1-24
                try:
                    interval_num = int(interval_value)
                    if interval_num < 1 or interval_num > 24:
                        logger.error(f"Hourly interval out of range (1-24): {interval_num}")
                        return False
                except ValueError:
                    logger.error(f"Invalid hourly interval format: {interval_value}")
                    return False
            elif time_res == "5min":
                # 5-minute: interval could be "HH:MM" format or numeric 1-288
                if isinstance(interval_value, str) and ":" in interval_value:
                    # Validate HH:MM format
                    parts = interval_value.split(":")
                    if len(parts) != 2:
                        logger.error(f"Invalid 5-minute interval format: {interval_value}")
                        return False
                    try:
                        hour = int(parts[0])
                        minute = int(parts[1])
                        if hour < 0 or hour > 23:
                            logger.error(f"Invalid hour in interval: {interval_value}")
                            return False
                        if minute not in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]:
                            logger.error(f"Invalid 5-minute increment: {interval_value}")
                            return False
                    except ValueError:
                        logger.error(f"Non-numeric values in interval: {interval_value}")
                        return False
                else:
                    # Numeric format: 1-288
                    try:
                        interval_num = int(interval_value)
                        if interval_num < 1 or interval_num > 288:
                            logger.error(f"5-minute interval out of range (1-288): {interval_num}")
                            return False
                    except ValueError:
                        logger.error(f"Invalid 5-minute interval: {interval_value}")
                        return False

            # Validate LMP components are numeric
            for component in ["lmp", "mcc", "mec", "mlc"]:
                if not isinstance(record[component], (int, float)):
                    logger.error(f"{component.upper()} value is not numeric: {record[component]}")
                    return False

            # Validate LMP arithmetic: LMP = MEC + MCC + MLC (within rounding tolerance)
            calculated_lmp = record["mec"] + record["mcc"] + record["mlc"]
            if abs(calculated_lmp - record["lmp"]) > 0.01:
                logger.warning(
                    f"LMP arithmetic mismatch for node {record['node']}: "
                    f"LMP={record['lmp']}, MEC+MCC+MLC={calculated_lmp:.2f}"
                )
                # This is a warning, not a validation failure

            # Validate date consistency
            expected_date = candidate.metadata.get('date')
            if time_interval.get('value') != expected_date:
                logger.error(
                    f"Date mismatch: expected {expected_date}, got {time_interval.get('value')}"
                )
                return False

            # Check for reasonable data volume
            record_count = len(data["data"])
            logger.info(f"Validated {record_count} forecasted records successfully")

            # Expected volumes based on resolution
            if time_res == "hourly":
                # Expect at least 1,000 records for a full day (3,000-5,000 nodes × 24 intervals)
                if record_count > 0 and record_count < 100:
                    logger.warning(f"Unexpectedly low record count for hourly data: {record_count}")
            elif time_res == "5min":
                # Expect at least 10,000 records for 5-minute data (much higher volume)
                if record_count > 0 and record_count < 1000:
                    logger.warning(f"Unexpectedly low record count for 5-minute data: {record_count}")

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
    "--time-resolution",
    type=click.Choice(["5min", "hourly"], case_sensitive=False),
    default="hourly",
    help="Time resolution: '5min' for 288 intervals/day or 'hourly' for 24 intervals/day"
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
    """Collect MISO Real-Time Ex-Ante LMP data from Pricing API.

    This scraper collects forecasted Locational Marginal Price data from MISO's Pricing API
    for the real-time energy market, including Ex-Ante (forecasted) LMP values and their
    components (MEC, MCC, MLC) for all commercial pricing nodes.

    Data is published at 7:00 AM EST on the operating day and continuously updated.

    Time Resolution Options:
    - hourly: 24 intervals per day, ~72K-120K records/day
    - 5min: 288 intervals per day, ~300K-400K records/day

    Examples:

        # Collect hourly data for January 2025
        python scraper_miso_rt_exante_lmp.py \\
            --api-key YOUR_KEY \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-31 \\
            --time-resolution hourly

        # Collect 5-minute resolution data
        python scraper_miso_rt_exante_lmp.py \\
            --api-key YOUR_KEY \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-01 \\
            --time-resolution 5min

        # Use environment variables for credentials
        export MISO_API_KEY=your_key
        export S3_BUCKET=your-bucket
        python scraper_miso_rt_exante_lmp.py \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-31

        # Force re-download existing data
        python scraper_miso_rt_exante_lmp.py \\
            --api-key YOUR_KEY \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-02 \\
            --force

        # Collect single day with debug logging
        python scraper_miso_rt_exante_lmp.py \\
            --api-key YOUR_KEY \\
            --start-date 2025-01-20 \\
            --end-date 2025-01-20 \\
            --log-level DEBUG
    """
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(
        "Starting MISO RT Ex-Ante LMP collection",
        extra={
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
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
    collector = MisoRealTimeExAnteLMPCollector(
        api_key=api_key,
        start_date=start_date,
        end_date=end_date,
        time_resolution=time_resolution,
        dgroup="miso_rt_exante_lmp",
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
