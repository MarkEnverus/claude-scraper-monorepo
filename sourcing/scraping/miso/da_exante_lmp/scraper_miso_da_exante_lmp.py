"""MISO Day-Ahead Ex-Ante LMP Scraper.

Collects Day-Ahead Ex-Ante Locational Marginal Pricing data from MISO's public market reports.
URL: https://docs.misoenergy.org/marketreports/{YYYYMMDD}_da_exante_lmp.csv

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-02

Features:
    - HTTP collection using BaseCollector framework
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive error handling and validation
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-02

import csv
import io
import logging
import os
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


class MisoDayAheadExAnteLMPCollector(BaseCollector):
    """Collector for MISO Day-Ahead Ex-Ante LMP data."""

    BASE_URL = "https://docs.misoenergy.org/marketreports"
    TIMEOUT_SECONDS = 30

    def __init__(self, start_date: datetime, end_date: datetime, **kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate candidates for each date in the range.

        MISO publishes one CSV file per day with date-based naming.
        """
        candidates = []
        current_date = self.start_date

        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y%m%d')
            identifier = f"da_exante_lmp_{date_str}.csv"
            url = f"{self.BASE_URL}/{date_str}_da_exante_lmp.csv"

            candidate = DownloadCandidate(
                identifier=identifier,
                source_location=url,
                metadata={
                    "data_type": "da_exante_lmp",
                    "source": "miso",
                    "date": current_date.strftime('%Y-%m-%d'),
                    "date_formatted": date_str,
                },
                collection_params={
                    "headers": {
                        "Accept": "text/csv",
                        "User-Agent": "MISO-DA-ExAnte-LMP-Collector/1.0",
                    },
                    "timeout": self.TIMEOUT_SECONDS,
                },
                file_date=current_date.date(),
            )

            candidates.append(candidate)
            logger.info(f"Generated candidate for date: {current_date.date()}")

            current_date += timedelta(days=1)

        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch CSV file from MISO."""
        logger.info(f"Fetching DA Ex-Ante LMP data from {candidate.source_location}")

        try:
            response = requests.get(
                candidate.source_location,
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            logger.info(f"Successfully fetched {len(response.content)} bytes")
            return response.content

        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch DA Ex-Ante LMP data: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate CSV structure of DA Ex-Ante LMP data."""
        try:
            text_content = content.decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(text_content))

            # Check for required columns
            if not csv_reader.fieldnames:
                logger.warning("No CSV headers found")
                return False

            required_fields = ["Node", "Value", "MarketDay", "HourEnding"]
            missing_fields = []
            for field in required_fields:
                if field not in csv_reader.fieldnames:
                    missing_fields.append(field)

            if missing_fields:
                logger.warning(f"Missing required fields: {missing_fields}")
                return False

            # Validate has data rows
            row_count = sum(1 for _ in csv_reader)
            if row_count == 0:
                logger.warning("No data rows in CSV")
                return False

            logger.info(f"Content validation passed ({row_count} data rows)")
            return True

        except (UnicodeDecodeError, csv.Error) as e:
            logger.error(f"Content validation error: {e}")
            return False


@click.command()
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Start date for collection (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="End date for collection (YYYY-MM-DD)",
)
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket name for storing data",
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
    start_date: datetime,
    end_date: datetime,
    s3_bucket: str,
    aws_profile: str,
    environment: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    log_level: str,
    kafka_connection_string: str,
):
    """Collect MISO Day-Ahead Ex-Ante LMP data.

    Example:
        python scraper_miso_da_exante_lmp.py \\
            --start-date 2025-01-01 --end-date 2025-01-31 \\
            --s3-bucket scraper-testing --aws-profile localstack
    """
    # Configure logging
    logging.getLogger("sourcing_app").setLevel(log_level)

    logger.info("Starting MISO Day-Ahead Ex-Ante LMP collection")
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    logger.info(f"S3 Bucket: {s3_bucket}")
    logger.info(f"Environment: {environment}")
    logger.info(f"AWS Profile: {aws_profile}")

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
    collector = MisoDayAheadExAnteLMPCollector(
        start_date=start_date,
        end_date=end_date,
        dgroup="miso_da_exante_lmp",
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
