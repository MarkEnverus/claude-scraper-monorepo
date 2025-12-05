"""MISO Sub-Regional Binding Constraints Scraper.

Collects Sub-Regional Binding Constraints data from MISO's Public API.
Endpoint: https://public-api.misoenergy.org/api/BindingConstraints/SubRegional

Data includes detailed sub-regional transmission constraint information:
- Localized constraint names and locations
- Sub-regional geographic breakdowns (Region, Subregion)
- Constraint types and utilization metrics
- Shadow pricing and economic impacts
- Real-time constraint values and limits

Data is stored to S3 with date partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP REST API collection using BaseCollector framework
    - No authentication required (public API)
    - Redis-based hash deduplication
    - S3 storage with date partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive error handling and validation
    - 5-minute update frequency matching API refresh rate
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

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


class MisoBindingConstraintsSubregionalCollector(BaseCollector):
    """Collector for MISO Sub-Regional Binding Constraints data."""

    BASE_URL = "https://public-api.misoenergy.org/api/BindingConstraints/SubRegional"
    TIMEOUT_SECONDS = 30

    def __init__(self, start_date: datetime, end_date: datetime, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date

    def generate_candidates(self, **kwargs: Any) -> List[DownloadCandidate]:
        """Generate candidates for each timestamp in the range.

        MISO publishes sub-regional binding constraints data with 5-minute updates.
        The API returns a snapshot of current constraint data including:
        - Constraint names and geographic breakdowns (Region, Subregion)
        - Constraint types (transmission capacity, voltage, etc.)
        - Shadow prices and economic impact ($/MWh)
        - Constraint values, limits, and utilization percentages
        - Override indicators for special operational conditions

        We collect snapshots at 5-minute intervals to capture constraint dynamics
        and identify localized congestion patterns.
        """
        candidates = []
        current_date = self.start_date

        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            date_compact = current_date.strftime('%Y%m%d')

            # Generate identifier with timestamp for uniqueness
            timestamp = current_date.strftime('%Y%m%d_%H%M%S')
            identifier = f"binding_constraints_subregional_{timestamp}.json"
            url = self.BASE_URL

            candidate = DownloadCandidate(
                identifier=identifier,
                source_location=url,
                metadata={
                    "data_type": "binding_constraints_subregional",
                    "source": "miso",
                    "date": date_str,
                    "date_formatted": date_compact,
                    "timestamp": timestamp,
                    "market_type": "real_time_constraints",
                    "constraint_scope": "subregional",
                },
                collection_params={
                    "headers": {
                        "Accept": "application/json",
                        "User-Agent": "MISO-Binding-Constraints-SubRegional-Collector/1.0",
                    },
                    "timeout": self.TIMEOUT_SECONDS,
                },
                file_date=current_date.date(),
            )

            candidates.append(candidate)
            logger.info(f"Generated candidate for timestamp: {timestamp}")

            # For real-time data, increment by 5 minutes to match update frequency
            current_date += timedelta(minutes=5)

        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch JSON data from MISO Public API."""
        logger.info(f"Fetching Sub-Regional Binding Constraints data from {candidate.source_location}")

        try:
            response = requests.get(
                candidate.source_location,
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            # Parse JSON response
            json_data = response.json()

            # Add collection metadata
            result = {
                "data": json_data,
                "collection_metadata": {
                    "collected_at": datetime.now().isoformat(),
                    "source_url": candidate.source_location,
                    **candidate.metadata
                }
            }

            # Log collection summary
            constraints = json_data.get('SubRegionalConstraints', [])
            ref_id = json_data.get('RefId', 'N/A')

            logger.info(
                f"Successfully collected Sub-Regional Binding Constraints data. "
                f"RefId: {ref_id}, "
                f"Constraints: {len(constraints)} active"
            )

            # Log additional details if constraints present
            if constraints:
                # Count by constraint type
                type_counts: Dict[str, int] = {}
                high_util_count = 0
                override_count = 0

                for constraint in constraints:
                    ctype = constraint.get('ConstraintType', 'UNKNOWN')
                    type_counts[ctype] = type_counts.get(ctype, 0) + 1

                    utilization = constraint.get('Utilization', 0)
                    if utilization >= 90:
                        high_util_count += 1

                    if constraint.get('OVERRIDE'):
                        override_count += 1

                logger.info(
                    f"Constraint breakdown: {type_counts}, "
                    f"High utilization (≥90%): {high_util_count}, "
                    f"Overrides: {override_count}"
                )

            return json.dumps(result, indent=2).encode('utf-8')

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning("API endpoint returned 404 - data may not be available")
                raise ScrapingError(f"Data not available: {e}") from e
            elif e.response.status_code == 429:
                logger.error("API rate limit exceeded (429)")
                raise ScrapingError(f"Rate limit exceeded: {e}") from e
            elif e.response.status_code == 503:
                logger.error("API service unavailable (503)")
                raise ScrapingError(f"Service unavailable: {e}") from e
            raise ScrapingError(f"HTTP error fetching Sub-Regional Binding Constraints data: {e}") from e
        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch Sub-Regional Binding Constraints data: {e}") from e
        except json.JSONDecodeError as e:
            raise ScrapingError(f"Invalid JSON response: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of Sub-Regional Binding Constraints data.

        Expected format:
        {
            "data": {
                "RefId": "05-Dec-2025 - Interval 10:30 EST",
                "SubRegionalConstraints": [
                    {
                        "Name": "MINN_METRO_WEST_01",
                        "Region": "Minnesota",
                        "Subregion": "Metro West",
                        "ConstraintType": "TRANSMISSION_CAPACITY",
                        "Period": "2025-12-05T10:30:00Z",
                        "Price": 12.50,
                        "ConstraintValue": 450.0,
                        "ConstraintLimit": 500.0,
                        "Utilization": 90.0,
                        "OVERRIDE": false
                    }
                ]
            }
        }
        """
        try:
            text_content = content.decode('utf-8')
            result = json.loads(text_content)

            # Check top-level structure
            if "data" not in result:
                logger.error("Missing 'data' field in response")
                return False

            data = result["data"]

            # Check required top-level fields
            if "RefId" not in data:
                logger.error("Missing 'RefId' field in data")
                return False

            if "SubRegionalConstraints" not in data:
                logger.error("Missing 'SubRegionalConstraints' array in data")
                return False

            constraints = data["SubRegionalConstraints"]

            if not isinstance(constraints, list):
                logger.error("SubRegionalConstraints is not a list")
                return False

            # If empty array, this is valid (no active constraints)
            if len(constraints) == 0:
                logger.info(f"No active sub-regional constraints at {data['RefId']}")
                return True

            # Validate structure of first constraint as representative sample
            constraint = constraints[0]

            # Check required fields
            required_fields = [
                "Name", "Region", "Subregion", "ConstraintType",
                "Period", "Price", "ConstraintValue", "ConstraintLimit",
                "Utilization", "OVERRIDE"
            ]

            for field in required_fields:
                if field not in constraint:
                    logger.error(f"Missing required field in constraint: {field}")
                    return False

            # Validate field types
            if not isinstance(constraint["Name"], str):
                logger.error(f"Name is not a string: {constraint['Name']}")
                return False

            if not isinstance(constraint["Region"], str):
                logger.error(f"Region is not a string: {constraint['Region']}")
                return False

            if not isinstance(constraint["Subregion"], str):
                logger.error(f"Subregion is not a string: {constraint['Subregion']}")
                return False

            if not isinstance(constraint["ConstraintType"], str):
                logger.error(f"ConstraintType is not a string: {constraint['ConstraintType']}")
                return False

            # Validate numeric fields
            numeric_fields = ["Price", "ConstraintValue", "ConstraintLimit", "Utilization"]
            for field in numeric_fields:
                if not isinstance(constraint[field], (int, float)):
                    logger.error(f"{field} is not numeric: {constraint[field]}")
                    return False

            # Validate utilization is within expected range (0-100%)
            utilization = constraint["Utilization"]
            if utilization < 0 or utilization > 100:
                logger.warning(
                    f"Utilization out of expected range (0-100%): {utilization}% "
                    f"for constraint {constraint['Name']}"
                )

            # Validate boolean OVERRIDE field
            if not isinstance(constraint["OVERRIDE"], bool):
                # Some APIs may use 0/1 instead of true/false
                if constraint["OVERRIDE"] not in [0, 1]:
                    logger.error(f"OVERRIDE is not boolean or 0/1: {constraint['OVERRIDE']}")
                    return False

            # Validate all constraints if more than one present
            for i, c in enumerate(constraints):
                for field in required_fields:
                    if field not in c:
                        logger.error(f"Constraint {i} missing required field: {field}")
                        return False

            logger.info(
                f"Validation successful: "
                f"RefId={data['RefId']}, "
                f"Constraints={len(constraints)} active"
            )
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
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    required=True,
    help="Start date/time for data collection (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    required=True,
    help="End date/time for data collection (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"
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
    """Collect MISO Sub-Regional Binding Constraints data.

    This scraper collects sub-regional transmission constraint data from MISO's
    Public API, providing granular insights into localized grid congestion:

    - Constraint names and geographic breakdowns (Region, Subregion)
    - Constraint types (transmission capacity, voltage limits, etc.)
    - Shadow prices indicating economic impact ($/MWh)
    - Constraint values, limits, and utilization percentages
    - Override indicators for special operational conditions

    Data is updated every 5 minutes and provides critical information for:
    - Localized congestion analysis
    - Regional market strategy
    - Grid planning and risk management
    - Transmission pricing impacts

    Examples:

        # Collect a single snapshot
        python scraper_miso_binding_constraints_subregional.py \\
            --start-date 2025-12-05T12:00:00 \\
            --end-date 2025-12-05T12:00:00

        # Collect data for one hour at 5-minute intervals
        python scraper_miso_binding_constraints_subregional.py \\
            --start-date 2025-12-05T12:00:00 \\
            --end-date 2025-12-05T13:00:00

        # Collect full day of constraint data
        python scraper_miso_binding_constraints_subregional.py \\
            --start-date 2025-12-05 \\
            --end-date 2025-12-06

        # Use environment variables for credentials
        export S3_BUCKET=your-bucket
        export REDIS_HOST=localhost
        python scraper_miso_binding_constraints_subregional.py \\
            --start-date 2025-12-05T12:00:00 \\
            --end-date 2025-12-05T14:00:00

        # Force re-download existing data
        python scraper_miso_binding_constraints_subregional.py \\
            --start-date 2025-12-05T12:00:00 \\
            --end-date 2025-12-05T12:30:00 \\
            --force

        # Production collection with specific environment
        python scraper_miso_binding_constraints_subregional.py \\
            --start-date 2025-12-05 \\
            --end-date 2025-12-06 \\
            --environment prod \\
            --log-level INFO
    """
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(
        "Starting MISO Sub-Regional Binding Constraints collection",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
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
    session_kwargs: Dict[str, str] = {}
    if aws_profile:
        session_kwargs["profile_name"] = aws_profile

    session = boto3.Session(**session_kwargs)
    s3_client = session.client("s3")

    if s3_bucket:
        logger.info(f"Using S3 bucket: {s3_bucket}")
    else:
        logger.warning("No S3 bucket specified - files will be validated but not uploaded")

    # Initialize collector
    collector = MisoBindingConstraintsSubregionalCollector(
        start_date=start_date,
        end_date=end_date,
        dgroup="miso_binding_constraints_subregional",
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
