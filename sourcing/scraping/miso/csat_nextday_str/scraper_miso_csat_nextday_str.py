"""MISO CSAT Next-Day Short-Term Reserve Requirement Scraper.

Collects next-day short-term reserve requirement data from MISO's public API.
Endpoint: https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement

This data provides the short-term reserve requirements for the next operating day,
including total requirements, reserve components (Regulation and Contingency),
subregional breakdowns, and qualitative assessments with uncertainty factors.

Data is published daily by 2:00 PM EST.

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP collection using BaseCollector framework
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive validation including arithmetic checks
    - Date-based historical collection support
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


class MisoCsatNextDaySTRCollector(BaseCollector):
    """Collector for MISO CSAT Next-Day Short-Term Reserve Requirement data."""

    API_URL = "https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement"
    TIMEOUT_SECONDS = 30

    def __init__(self, start_date: datetime, end_date: datetime, **kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate candidates for each date in the range.

        MISO publishes next-day STR data daily by 2:00 PM EST. Each date returns
        JSON with total requirements, reserve components, subregional breakdowns,
        and qualitative assessments.
        """
        candidates = []
        current_date = self.start_date

        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y-%m-%d')  # API expects YYYY-MM-DD
            date_compact = current_date.strftime('%Y%m%d')  # For identifier
            identifier = f"csat_nextday_str_{date_compact}.json"

            candidate = DownloadCandidate(
                identifier=identifier,
                source_location=self.API_URL,
                metadata={
                    "data_type": "csat_nextday_str",
                    "source": "miso",
                    "date": date_str,
                    "date_formatted": date_compact,
                },
                collection_params={
                    "headers": {
                        "Accept": "application/json",
                        "User-Agent": "MISO-CSAT-NextDay-STR-Collector/1.0",
                    },
                    "timeout": self.TIMEOUT_SECONDS,
                    "query_params": {
                        "date": date_str,
                    }
                },
                file_date=current_date.date(),
            )

            candidates.append(candidate)
            logger.info(f"Generated candidate for date: {current_date.date()}")

            current_date += timedelta(days=1)

        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch JSON data from MISO CSAT API."""
        date_param = candidate.collection_params.get("query_params", {}).get("date")
        logger.info(f"Fetching CSAT Next-Day STR data for date: {date_param}")

        try:
            response = requests.get(
                candidate.source_location,
                params=candidate.collection_params.get("query_params", {}),
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            logger.info(f"Successfully fetched {len(response.content)} bytes")
            return response.content

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                logger.error(f"Bad request - invalid date format: {date_param}")
            elif e.response.status_code == 404:
                logger.warning(f"No data available for date: {date_param}")
                # 404 is not an error - data may not exist for this date yet
                raise ScrapingError(f"No data available for date {date_param}") from e
            elif e.response.status_code == 429:
                logger.warning("Rate limit exceeded")
            raise ScrapingError(f"HTTP error fetching CSAT data: {e}") from e
        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch CSAT data: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure and arithmetic consistency of CSAT data.

        Expected format:
        {
            "operatingDay": "2025-01-20",
            "publishedTimestamp": "2025-01-19T14:00:00Z",
            "totalShortTermReserveRequirement": 2500.5,
            "reserveComponents": [
                {"type": "RegulationReserve", "value": 800.0, "unit": "MW"},
                {"type": "ContingencyReserve", "value": 1700.5, "unit": "MW"}
            ],
            "subregions": [
                {
                    "name": "North",
                    "shortTermReserveRequirement": 1200.0,
                    "reserveDeficit": 0.0
                }
            ],
            "qualitativeAssessment": {
                "overallAdequacy": "Adequate",
                "uncertaintyFactor": 0.15,
                "recommendedActions": ["Monitor wind forecasts"]
            }
        }

        Validation checks:
        - Verify operatingDay matches requested date
        - totalShortTermReserveRequirement = sum of reserveComponents values
        - totalShortTermReserveRequirement = sum of subregions requirements
        - uncertaintyFactor between 0.0 and 1.0
        - All MW values non-negative
        """
        try:
            data = json.loads(content)

            # Check required top-level fields
            required_fields = [
                "operatingDay",
                "publishedTimestamp",
                "totalShortTermReserveRequirement",
                "reserveComponents",
                "subregions",
                "qualitativeAssessment"
            ]
            for field in required_fields:
                if field not in data:
                    logger.error(f"Missing required field: {field}")
                    return False

            # Validate operatingDay matches requested date
            expected_date = candidate.metadata.get("date")
            if data["operatingDay"] != expected_date:
                logger.error(
                    f"Operating day mismatch: expected {expected_date}, got {data['operatingDay']}"
                )
                return False

            # Validate totalShortTermReserveRequirement is non-negative
            total_str = data["totalShortTermReserveRequirement"]
            if not isinstance(total_str, (int, float)) or total_str < 0:
                logger.error(f"Invalid totalShortTermReserveRequirement: {total_str}")
                return False

            # Validate reserveComponents structure and sum
            reserve_components = data["reserveComponents"]
            if not isinstance(reserve_components, list) or len(reserve_components) == 0:
                logger.error("Invalid or empty reserveComponents array")
                return False

            components_sum = 0.0
            for component in reserve_components:
                required_component_fields = ["type", "value", "unit"]
                for field in required_component_fields:
                    if field not in component:
                        logger.error(f"Missing field in reserve component: {field}")
                        return False

                # Validate component value is non-negative
                if not isinstance(component["value"], (int, float)) or component["value"] < 0:
                    logger.error(f"Invalid reserve component value: {component['value']}")
                    return False

                # Validate unit is MW
                if component["unit"] != "MW":
                    logger.error(f"Invalid unit in reserve component: {component['unit']}")
                    return False

                components_sum += component["value"]

            # Validate arithmetic: total = sum of components (with tolerance for floating point)
            if abs(components_sum - total_str) > 0.01:
                logger.error(
                    f"Reserve components sum mismatch: total={total_str}, "
                    f"components_sum={components_sum:.2f}"
                )
                return False

            # Validate subregions structure and sum
            subregions = data["subregions"]
            if not isinstance(subregions, list) or len(subregions) == 0:
                logger.error("Invalid or empty subregions array")
                return False

            subregions_sum = 0.0
            for subregion in subregions:
                required_subregion_fields = ["name", "shortTermReserveRequirement", "reserveDeficit"]
                for field in required_subregion_fields:
                    if field not in subregion:
                        logger.error(f"Missing field in subregion: {field}")
                        return False

                # Validate requirement is non-negative
                requirement = subregion["shortTermReserveRequirement"]
                if not isinstance(requirement, (int, float)) or requirement < 0:
                    logger.error(f"Invalid subregion requirement: {requirement}")
                    return False

                # Validate deficit is non-negative
                deficit = subregion["reserveDeficit"]
                if not isinstance(deficit, (int, float)) or deficit < 0:
                    logger.error(f"Invalid subregion deficit: {deficit}")
                    return False

                subregions_sum += requirement

            # Validate arithmetic: total = sum of subregions (with tolerance)
            if abs(subregions_sum - total_str) > 0.01:
                logger.error(
                    f"Subregions sum mismatch: total={total_str}, "
                    f"subregions_sum={subregions_sum:.2f}"
                )
                return False

            # Validate qualitativeAssessment structure
            assessment = data["qualitativeAssessment"]
            required_assessment_fields = ["overallAdequacy", "uncertaintyFactor", "recommendedActions"]
            for field in required_assessment_fields:
                if field not in assessment:
                    logger.error(f"Missing field in qualitativeAssessment: {field}")
                    return False

            # Validate uncertaintyFactor is between 0.0 and 1.0
            uncertainty = assessment["uncertaintyFactor"]
            if not isinstance(uncertainty, (int, float)) or uncertainty < 0.0 or uncertainty > 1.0:
                logger.error(f"Invalid uncertaintyFactor (must be 0.0-1.0): {uncertainty}")
                return False

            # Validate recommendedActions is a list
            if not isinstance(assessment["recommendedActions"], list):
                logger.error("recommendedActions must be a list")
                return False

            logger.info(
                f"Validation passed: operatingDay={data['operatingDay']}, "
                f"totalSTR={total_str} MW, "
                f"components={len(reserve_components)}, "
                f"subregions={len(subregions)}, "
                f"uncertaintyFactor={uncertainty}"
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
    """Collect MISO CSAT Next-Day Short-Term Reserve Requirement data.

    This scraper collects next-day short-term reserve requirement data from MISO's
    public API, including total requirements, reserve components, subregional breakdowns,
    and qualitative assessments with uncertainty factors.

    Data is published daily by 2:00 PM EST for the next operating day.

    Examples:

        # Collect data for January 2025
        python scraper_miso_csat_nextday_str.py \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-31

        # Use environment variables for AWS
        export S3_BUCKET=your-bucket
        export AWS_PROFILE=localstack
        python scraper_miso_csat_nextday_str.py \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-31

        # Force re-download existing data
        python scraper_miso_csat_nextday_str.py \\
            --start-date 2025-01-01 \\
            --end-date 2025-01-02 \\
            --force

        # Collect single day with debug logging
        python scraper_miso_csat_nextday_str.py \\
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
        "Starting MISO CSAT Next-Day STR collection",
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
    collector = MisoCsatNextDaySTRCollector(
        start_date=start_date,
        end_date=end_date,
        dgroup="miso_csat_nextday_str",
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
